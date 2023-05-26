第一个编程项目是在存储管理器中实现一个缓冲池。缓冲池负责将物理页面从主内存来回移动到磁盘。它允许DBMS支持大于系统可用内存量的数据库。缓冲池的操作对系统中的其他部分是透明的。例如，系统使用页面的唯一标识符（page_id_t）向缓冲池请求页面，但它不知道该页面是否已经在内存中，也不知道系统是否必须从磁盘中检索该页面。



您的实现需要是线程安全的。多个线程将同时访问内部数据结构，因此您需要确保它们的关键部分受到锁存器的保护（这些锁存器在操作系统中被称为“锁”）。

您需要在存储管理器中实现以下两个组件：

- [**LRU Replacement Policy**](https://15445.courses.cs.cmu.edu/fall2021/project1/#replacer)
- [**Buffer Pool Manager Instance**](https://15445.courses.cs.cmu.edu/fall2021/project1/#buffer-pool-instance)
- [**Parallel Buffer Pool Manager**](https://15445.courses.cs.cmu.edu/fall2021/project1/#parallel-buffer-pool)

# TASK #1 - LRU REPLACEMENT POLICY

此组件负责跟踪缓冲池中的页面使用情况。您将在**src/include/buffer/lru_replacer.h**中实现一个名为LRUReplacer的新子类，并在**src/buffer/lru_replacer.cpp**中实现相应的实现文件。LRUReplacer扩展了抽象的replacer类（**src/include/pauffer/replacer.h**），其中包含函数规范。



LRUReplacer的最大页数与缓冲池的大小相同，因为它包含BufferPoolManager中所有帧的占位符。然而，在任何给定时刻，并不是所有的帧都被认为在LRUReplacer中。LRUReplacer被初始化为其中没有帧。然后，只有新取消固定的帧才会被认为在LRUReplacer中。



您将需要实施课程中讨论的LRU策略。您需要实现以下方法：

- Victim（frame_id_t*）：删除与Replacer正在跟踪的所有其他元素相比最近访问次数最少的对象，将其内容存储在输出参数中并返回True。如果Replacer为空，则返回False。

- Pin（frame_id_t）：将页面固定到BufferPoolManager中的帧后，应调用此方法。它应该从LRUReplacer中删除包含固定页面的框架。

- Unpin（frame_id_t）：当页面的pin_count变为0时，应调用此方法。此方法应将包含未固定页面的框架添加到LRUReplacer。

- Size（）：此方法返回当前在LRUReplacer中的帧数。


具体实施细节由您决定。您可以使用内置的STL容器。您可以假设不会耗尽内存，但必须确保操作是线程安全的。

最后，在这个项目中，您只需要执行LRU更换政策。您不必实现时钟替换策略，即使有相应的文件。





## lru_replacer.h

- `Victim(frame_id_t*)`：驱逐缓冲池中最近最少使用的页面，并将其内容存储在输入参数中。当`LRUReplacer`为空时返回False，否则返回True；
- `Pin(frame_id_t)`：当缓冲池中的页面被用户访问时，该方法被调用使得该页面从`LRUReplacer`中驱逐，以使得该页面固定在缓存池中；
- `Unpin(frame_id_t)`：当缓冲池的页面被所有用户使用完毕时，该方法被调用使得该页面被添加在`LRUReplacer`，使得该页面可以被缓冲池驱逐；
- `Size()`：返回`LRUReplacer`中页面的数目；

```c++
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

  //void DeleteNode(LinkListNode *curr);


```

在这里，LRU策略可以由哈希表加双向链表的方式实现：

- **链表充当队列的功能以记录页面被访问的先后顺序，**
- **哈希表则记录<页面ID - 链表节点>键值对，以在O(1)复杂度下删除链表元素。**

实际实现中使用STL中的哈希表unordered_map和双向链表list，并在unordered_map中存储指向链表节点的list::iterator。

```c++
 private:
  // TODO(student): implement me!
  std::list<frame_id_t> lru_list_;	//链表
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> data_idx_;  //哈希表
  std::mutex data_latch_;
};
```

## lru_replacer.cpp

### victim

victim驱逐缓冲池中最近最少使用的页面，并将其内容存储在输入参数中。

**在此函数中，自动给frame_id赋值**

对于`Victim`：

- 首先判断链表是否为空
- 如不为空则返回链表尾节点的页面ID
- 并在哈希表中解除指向尾节点的映射

为了保证线程安全，整个函数应当由`mutex`互斥锁保护，下文中对互斥锁操作不再赘述。

```c++
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(data_latch_);      // 上锁

  if (data_idx_.empty()) {  // 首先判断链表是否为空
    return false;           // 返回false
  }

  *frame_id = lru_list_.back();  // 获得lru_list尾节点
  lru_list_.pop_back();			 // 删除链表尾节点
  data_idx_.erase(*frame_id);    // 通过地址 *frame_id_t 在哈希表中解除指向首节点的映射。
  return true;
}
```

PS：l**ock_guard：创建时加锁，析构时解锁。**

作用：为了防止在线程使用mutex加锁后异常退出导致死锁的问题，建议使用lock_guard代替mutex。

lock_guard的构造函数：
    explicit lock_guard (mutex_type& m);

- lock_guard类通过在对象构造的时候对mutex进行加锁，当对象离开作用域时自动解锁，从而避免加锁后没有解锁的问题。
- lock_guard不能在中途解锁，只能通过析构时解锁。
- lock_guard对象不能被拷贝和移动。

### Pin

当缓冲池中的页面被用户访问时，该方法被调用使得该页面从`LRUReplacer`中驱逐，以使得该页面固定在缓存池中

对于`Pin`

- 检查`LRUReplace`中是否存在对应页面ID的节点，
- 如不存在则直接返回，
- 如存在对应节点则通过哈希表中存储的迭代器删除链表节点，并解除哈希表对应页面ID的映射。

```c++
 void LRUReplacer::Pin(frame_id_t frame_id) { 
   std::lock_guard<std::mutex> lock(data_latch_);   //上锁
   auto it = data_idx_.find(frame_id);              //寻找是否存在对应页面ID节点
   if (it == data_idx_.end()) {                     //不存在
      return;                                       //直接返回
   }                                                //如果存在
   lru_list_.erase(it->second);                     //通过哈希表存储的迭代器删除链表节点
   data_idx_.erase(it);                             //解除哈希表对应的页面ID的映射
 }
```

### Unpin

当缓冲池的页面被所有用户使用完毕时，该方法被调用使得该页面被添加在`LRUReplacer`，使得该页面可以被缓冲池驱逐；

对于`Unpin`

- 检查`LRUReplace`中是否存在对应页面ID的节点，
- 如存在则直接返回，
- 如不存在则在链表尾部插入页面ID的节点，并在哈希表中插入<页面ID - 链表尾节点>映射。

```c++
 void LRUReplacer::Unpin(frame_id_t frame_id) { 
     std::lock_guard<std::mutex> lock(data_latch_); //上锁
     auto it = data_idx_.find(frame_id);            //寻找是否存在对应页面ID的节点
     if (it != data_idx_.end()) {                   //如果存在（已取消固定）
        return;                                     //直接返回
     }                                              //如果不存在
     lru_list_.emplace_front(frame_id);             //在链表首部插入页面ID节点
     data_idx_[frame_id] = lru_list_.front();       //在哈希表中插入<页面ID - 链表尾节点>映射
 }
```

### `Size`

返回哈希表大小即可。

```c++
size_t LRUReplacer::Size() { 
    std::lock_guard<std::mutex> lock(data_latch_);  //上锁
    size_t ret = data_idx_.size();                  //获取哈希表大小
    return ret;                                     //返回结果
}
```

# TASK #2 - BUFFER POOL MANAGER INSTANCE

在该部分中，需要实现缓冲池管理模块，其从`DiskManager`中获取数据库页面，并在缓冲池强制要求时或驱逐页面时将数据库脏页面写回`DiskManager`。

## Page.h

```c++
class Page {
  // There is book-keeping information inside the page that should only be relevant to the buffer pool manager.
  friend class BufferPoolManagerInstance;

 public:
  /** Constructor. Zeros out the page data. */
  Page() { ResetMemory(); }

  /** Default destructor. */
  ~Page() = default;

  /** @return the actual data contained within this page */
  inline auto GetData() -> char * { return data_; }

  /** @return the page id of this page */
  inline auto GetPageId() -> page_id_t { return page_id_; }

  /** @return the pin count of this page */
  inline auto GetPinCount() -> int { return pin_count_; }

  /** @return true if the page in memory has been modified from the page on disk, false otherwise */
  inline auto IsDirty() -> bool { return is_dirty_; }

  /** Acquire the page write latch. */
  inline void WLatch() { rwlatch_.WLock(); }

  /** Release the page write latch. */
  inline void WUnlatch() { rwlatch_.WUnlock(); }

  /** Acquire the page read latch. */
  inline void RLatch() { rwlatch_.RLock(); }

  /** Release the page read latch. */
  inline void RUnlatch() { rwlatch_.RUnlock(); }

  /** @return the page LSN. */
  inline auto GetLSN() -> lsn_t { return *reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN); }

  /** Sets the page LSN. */
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t)); }

 protected:
  static_assert(sizeof(page_id_t) == 4);
  static_assert(sizeof(lsn_t) == 4);

  static constexpr size_t SIZE_PAGE_HEADER = 8;
  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t OFFSET_LSN = 4;

 private:
    
  //将页面中保存的数据归零。
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

  //data_保存对应磁盘页面的实际数据
  char data_[PAGE_SIZE]{};

  //page_id_保存该页面在磁盘管理器中的页面ID
  page_id_t page_id_ = INVALID_PAGE_ID;

  //pin_count_保存DBMS中正使用该页面的用户数目
  int pin_count_ = 0;

  //is_dirty_保存该页面自磁盘读入或写回后是否被修改
  bool is_dirty_ = false;
    
  /** Page latch. */
  ReaderWriterLatch rwlatch_;
};
```

- ResetMemory()将页面中保存的数据归零
- `Page`是缓冲池中的页面容器
- `data_`保存对应磁盘页面的实际数据；
- `page_id_`保存该页面在磁盘管理器中的页面ID；
- `pin_count_`保存DBMS中正使用该页面的用户数目；
- `is_dirty_`保存该页面自磁盘读入或写回后是否被修改。

## buffer_pool_manager_instance.h

```c++
class BufferPoolManagerInstance : public BufferPoolManager {

  static const frame_id_t NUMLL_FRAME = -1;

  //缓冲池中的页数。
  const size_t pool_size_;

  //并行Buffer_Pool_Manager(BPM)中有多少实例（如果存在并行缓冲池，否则只有1个BPI）
  const uint32_t num_instances_ = 1;

  //并行buffer_pool_manager(BPM)中此buffer_pool_manager_instance(BPI)的索引（除非存在，否则仅为0）
  const uint32_t instance_index_ = 0;

  //每个BPI都为要分发的page_id维护自己的计数器，必须确保它们变回其instance_index_
  std::atomic<page_id_t> next_page_id_ = instance_index_;

  //pages_为缓冲池中的实际容器页面槽位数组，用于存放从磁盘中读入的页面，并供DBMS访问
  Page *pages_;

  //disk_manager_为磁盘管理器，提供从磁盘读入页面及写入页面的接口
  DiskManager *disk_manager_ __attribute__((__unused__));

  //log_manager_为日志管理器，本实验中不用考虑该组件
  LogManager *log_manager_ __attribute__((__unused__));

  //页表(page_table_)用于保存磁盘页面IDpage_id和槽位IDframe_id_t的映射
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  //轮换器(raplacer_)用于选取所需驱逐的页面
  Replacer *replacer_;

  //保存缓冲池中的空闲槽位ID
  std::list<frame_id_t> free_list_;
    
  //此锁存器保护共享数据结构
  std::mutex latch_;
};
```

缓冲池的成员如上所示，

- pool_size_ 为缓冲池中的页数
- num_instances为并行BPM中有多少实例（除非存在，否则只有1个BPI）
- instance_index_为并行BPM中此BPI的索引（除非存在，否则仅为0）
- next_page_id_为每个BPI都为要分发的page_id维护自己的计数器，必须确保它们变回其instance_index_
- `pages_`为缓冲池中的实际容器页面槽位数组，用于存放从磁盘中读入的页面，并供DBMS访问；
- `disk_manager_`为磁盘管理器，提供从磁盘读入页面及写入页面的接口；
- `log_manager_`为日志管理器，本实验中不用考虑该组件；
- `page_table_`用于保存磁盘页面ID`page_id`和槽位ID`frame_id_t`的映射；
- `raplacer_`用于选取所需驱逐的页面；
- `free_list_`保存缓冲池中的空闲槽位ID。
- latch_ 用于保护线程安全，上锁

在这里，区分`page_id`和`frame_id_t`是完成本实验的关键。

## buffer_pool_manager_instance.cpp

### BufferPoolManagerInstance构造/析构函数

```c++
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),            //pool_size_ = pool_size
      num_instances_(num_instances),    //num_instances_ = num_instances
      instance_index_(instance_index),  //instance_index_ = instance_index
      next_page_id_(instance_index),    //next_page_id_ = next_page_id
      disk_manager_(disk_manager),      //disk_manager_ = disk_manager
      log_manager_(log_manager) {       //log_manager_ = log_manager
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");  //如果BPI不是池的一部分，那么池大小应该只有1
  BUSTUB_ASSERT(
      instance_index < num_instances,
      //BPI索引不能大于池中的BPI数。在非并行情况下，索引应仅为1。
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");

  // 我们为缓冲池分配一个连续的内存空间。
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // 最初，每个页面都在空闲槽位free_list_中。
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

// 析构函数
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}
```

### FlushPgImp

`FlushPgImp`用于显式地将缓冲池页面写回磁盘。

- 首先，应当检查缓冲池中是否存在对应页面ID的页面，如不存在则返回False；
- 如存在对应页面，则将缓冲池内的该页面的`is_dirty_`置为false，并使用`WritePage`将该页面的实际数据`data_`写回磁盘。

在这里，需要使用互斥锁保证线程安全，在下文中不再赘述。

```c++
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) { 
    frame_id_t frame_id;
    std::lock_guard<std::mutex> lock(latch_);                       //上锁
    if (page_table_.count(page_id) == 0) {                          //若缓冲池中不存在对应页面ID页面
        return false;                                               //返回false
    }                                                               //若存在
    frame_id = page_table_[page_id];                                //获取对应页面的页框ID
    pages_[frame_id].is_dirty_ = false;                             //将缓冲池内的该页面的is_dirty_置为false
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());  //使用WritePage将该页面的实际数据data_写回磁盘
    return true;                                                    //返回true
}
```

### FlushAllPgsImp

`FlushAllPgsImp`将缓冲池内的所有页面写回磁盘。

- 遍历`page_table_`以获得缓冲池内的<页面ID - 槽位ID>对

- 通过槽位ID获取实际页面，并将页面的is_dirty置为false

- 通过页面ID作为写回磁盘的参数。

- ```c++
    void BufferPoolManagerInstance::FlushAllPgsImp() { 
      std::lock_guard<std::mutex> lock(latch_);     // 上锁
      for (auto id : page_table_) {                 // 遍历page_table_以获得缓冲池内的<页面ID - 槽位ID>对
          pages_[id.second].is_dirty_ = false;      // 将缓冲池内所有页面的is_dirty_置为false
          disk_manager_->WritePage(id.first, pages_[id.second].GetData());    //使用WritePage将该页面的实际数据data_写回磁盘
      }
    }
  ```

#### GetFrame()

 GetFrame()将 获取frame_id，若分配的是空余页，直接返回frame_id；若分配的是被驱逐页，**被驱逐页如为脏页则写入磁盘**，随后返回frame_id。

进入函数前需加锁

- 检查free_list_是否为空

- 如果free_list_不为空（存在空余页），将 frame_id设置为 free_list_尾节点的值，free_list_删除尾节点

- 如果free_list_为空（不存在空余页）,调用Victim()给frame_id赋值

  - 若无可驱逐页面，赋值失败，返回NUMLL_FRAME
  - 如存在目标槽位，通过pages_获得对应的page_id页面ID。
    - 如果驱逐页面为脏页，则将脏页写入磁盘中
    - 随后擦除哈希表（page_table_）中<页面ID - 链表尾节点>映射

  ```c++
  frame_id_t BufferPoolManagerInstance::GetFrame() { 
          frame_id_t new_frame_id;
          if (!free_list_.empty()) {                                  //如果free_list_不为空（存在空余页）
              new_frame_id = free_list_.back();                       //将 frame_id设置为 free_list_尾节点的值
              free_list_.pop_back();                                  //free_list_删除尾节点
          }
          bool res = replacer_->Victim(&new_frame_id);                //如果free_list_为空（不存在空余页）,调用Victim()给frame_id赋值
          if (!res) {                                                 //若无可驱逐页面，赋值失败
              return NUMLL_FRAME;                                     //返回NUMLL_FRAME
          }
          page_id_t victim_page_id = pages_[new_frame_id].page_id_;   //获取被驱逐页面之前的页面ID
          if (pages_[new_frame_id].IsDirty()) {                       //如果被驱逐页面为脏页
              disk_manager_->WritePage(victim_page_id, pages_[new_frame_id].GetData());   //写入磁盘
          }       
          page_table_.erase(victim_page_id);                          //擦除哈希表（page_table_）中<页面ID - 链表尾节点>映射
  
          return new_frame_id;                                        //返回新页框ID
  ```

#### AllocatePage()

AllocatePage() 在磁盘上分配页面

```c++
/*
  并行BPM中有多少实例（除非存在，否则只有1个BPI）
  const uint32_t num_instances_ = 1;

  并行buffer_pool_manager(BPM)中此buffer_pool_manager_instance(BPI)的索引（除非存在，否则仅为0）
  const uint32_t instance_index_ = 0;

  每个BPI都为要分发的page_id维护自己的计数器，必须确保它们变回其instance_index_
  std::atomic<page_id_t> next_page_id_ = instance_index_;
*/

/*
	获取本页的page_id
	page_id + 实例数量 即为下一页真正的page_id
	验证此页面ID是否有效
	如有效则返回下页有效页面ID
*/
page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);   //验证分配的页面修改回此BPI
}
```



### NewPgImp

`NewPgImp`在磁盘中分配新的物理页面，将其添加至缓冲池，并返回指向缓冲池页面`Page`的指针。

- 调用**GetFrame()**检查当前缓冲池中是否存在空闲槽位或存放页面可被驱逐的槽位，

- 如**GetFrame()**返回**NUMLL_FRAME（即没有空闲槽位和可被驱逐槽位）**，则返回空指针；

- 如存在可被驱逐槽位，则调用`AllocatePage()`为新的物理页面分配`page_id`页面ID。

- 从`page_table_`中删除目标槽位中的原页面ID的映射，并将新的<页面ID - 槽位ID>映射插入，

- 然后更新槽位中页面的元数据。

- 将页面数据写入磁盘

- 返回指向缓冲池页面`Page`的指针

  **PS:**需要注意的是，在这里由于我们返回了指向该页面的指针，我们需要将该页面的用户数`pin_count_`置为1，并调用`replacer_`的`Pin`。

```c++
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) { 
    std::lock_guard<std::mutex> lock(latch_);       //上锁
    frame_id_t new_frame_id;
    page_id_t new_page_id;
    new_frame_id = GetFrame();                      //调用GetFrame()获取frame_id
    if (new_frame_id == NUMLL_FRAME) {              //如果没有空闲槽位和可被驱逐槽位
        return nullptr;                             //返回空指针
    }
    new_page_id = AllocatePage();   //如存在可被驱逐槽位，则调用`AllocatePage()`为新的物理页面分配`page_id`页面ID。

    page_table_[new_page_id] = new_frame_id;        //从`page_table_`中删除目标槽位中的原页面ID的映射，并将新的<页面ID - 槽位ID>映射插入

    pages_[new_frame_id].page_id_ = new_page_id;    //更新槽位中页面的元数据
    pages_[new_frame_id].is_dirty_ = false;
    pages_[new_frame_id].pin_count_ = 1;
    pages_[new_frame_id].ResetMemory();             //将页面中保存的数据归零。
    //此时新页内无数据，所以无需将数据从磁盘写入缓冲池

     /*
     创建新页也需要写回磁盘，如果不这样 newpage unpin 然后再被淘汰出去 fetchpage时就会报错
     （磁盘中并无此页）但不能直接is_dirty_置为true，测试会报错
     */
    disk_manager_->WritePage(new_page_id, pages_[new_frame_id].GetData());

    return &pages_[new_frame_id];					 //返回指向缓冲池页面Page的指针
}
```

### FetchPgImp

FetchPgImp的功能是获取对应页面ID的页面，并返回指向该页面的指针

- 首先，通过检查page_table_以检查缓冲池中是否已经缓冲该页面，
- 如果已经缓冲该页面，则直接返回该页面，并将该页面的用户数pin_count_递增以及调用replacer_的Pin方法；
- 如缓冲池中尚未缓冲该页面，则需寻找当前缓冲池中是否存在空闲槽位或存放页面可被驱逐的槽位
- 如存在可被驱逐槽位，则从`page_table_`中删除目标槽位中的原页面ID的映射，并将新的<页面ID - 槽位ID>映射插入，
- 然后更新槽位中页面的元数据。
- 将页面数据写入缓冲池

**PS:**该流程与NewPgImp中的对应流程相似，唯一不同的则是传入目标槽位的page_id为函数参数而非由AllocatePage()分配得到。

```c++
//FetchPgImp的功能是获取对应页面ID的页面，并返回指向该页面的指针
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id){ 
    std::lock_guard<std::mutex> lock(latch_);       //上锁
    frame_id_t frame_id;
    if (page_table_.count(page_id) > 0) {           //如果缓冲池存在对应页面
        frame_id = page_table_[page_id];            //获取对应页面ID
        if (pages_[frame_id].pin_count_ == 0) {     //如果之前页面没有用户，调用Pin方法固定页面防止被驱逐
            replacer_.Pin(frame_id);                //如果有用户，则已经固定
        }
        pages_[frame_id].pin_count_++;              //将该页面的用户数递增
        return &pages_[frame_id];
    }
    frame_id = GetFrame();                          //如果缓冲池没有对应页面，调用GetFrame()
    if (frame_id == NUMLL_FRAME) {                  //如果没有空闲页面和可驱逐页面
        return nullptr;                             //返回nullptr
    }
                                                    
    page_table_[page_id] = frame_id;                //更新哈希表的映射
    
    pages_[frame_id].is_dirty_ = false;             //更新槽位中页面的元数据
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].page_id_ = page_id;

    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());   //将页面数据写入缓冲池
    return &pages_[frame_id];                       // 返回指向缓冲池页面Page的指针
} 
```

### DeletePgImp

`DeletePgImp`的功能为从缓冲池中删除对应页面ID的页面，并将其插入空闲链表`free_list_`

- 首先，检查该页面是否存在于缓冲区，如未存在则返回True。
- 然后，检查该页面的用户数`pin_count_`是否为0，如非0则返回False。
- 在这里，不难看出`DeletePgImp`的返回值代表的是该页面是否被用户使用，因此在该页面不在缓冲区时也返回True；
- 然后，在`page_table_`中删除该页面的映射
- 调用Pin()将该页面从`LRUReplacer`中驱逐
- 将槽位ID插入空闲链表尾部。
- 更新槽位中页面的元数据。
- 返回true

```c++
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);   // 上锁
    frame_id_t frame_id;
    if (page_table_.count(page_id) == 0) {      //检查该页面是否存在于缓冲区
        return true;                            //如未存在则返回True
    }
    frame_id = page_table_[page_id];            //获取frame_id

    if (pages_[frame_id].pin_count_ != 0) {     //检查该页面的用户数`pin_count_`是否为0
        return false;                           //如非0则返回False
    }

    /*
    不需要写回页，该页已删除
    if (pages_[frame_id].IsDirty) {
        pages_[frame_id].is_dirty_ = false;
        disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }
    */

    page_table_.erase(page_id);                     //删除哈希表的映射
    replacer_->Pin(frame_id);                       //固定缓冲区页面ID
    free_list_.emplace_back(frame_id);              //在空余链表尾部添加节点

    pages_[frame_id].page_id_ = INVALID_PAGE_ID;    //更新槽位中页面的元数据
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;

    DeallocatePage(page_id);                        //删除磁盘上对应的页面数据

    return true;
}
```

### UnpinPgImp

UnpinPgImp的功能为提供用户向缓冲池通知页面使用完毕的接口，用户需声明使用完毕页面的页面ID以及使用过程中是否对该页面进行修改。

- 首先，需检查该页面是否在缓冲池中，如未在缓冲池中则返回True。
- 然后，检查该页面的用户数是否大于0，如不存在用户则返回false；
- 递减该页面的用户数pin_count_，
- 如在递减后该值等于0，则调用replacer_->Unpin以表示该页面可以被驱逐。

```c++
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);   //上锁
    frame_id_t frame_id;
    if (page_table_.count(page_id) == 0) {      //需检查该页面是否在缓冲池中
        return true;                            //如未在缓冲池中则返回True
    }
    frame_id = page_table_[page_id];            //获取frame_id
    Page curpage = pages_[frame_id];            //获取页面
    if (curpage.pin_count_ == 0) {              //检查该页面的用户数是否大于0
        return false;                           //如不存在用户则返回false
    }

    if (is_dirty) {                             //如果用户使用过程中对该页面进行了修改
        curpage.is_dirty_ = true;               //将该页面设置为脏页
    }

    curpage.pin_count_--;                       //递减该页面的用户数pin_count_

    if (curpage.pin_count_ == 0) {              //如在递减后该值等于0
        replacer_->Unpin(frame_id);             //调用replacer_->Unpin以表示该页面可以被驱逐
    }
    return true;                                //返回True
}       
```

# TASK #3 - PARALLEL BUFFER POOL MANAGER

## parallel_buffer_pool_manager.h

```c++
//GetPoolSize应返回全部缓冲池的容量，即独立缓冲池个数乘以缓冲池容量。
size_t ParallelBufferPoolManager::GetPoolSize() {
  // 获取所有BufferPoolManager实例的大小
  return num_instances_ * pool_size_;  // 缓冲池个数 * 缓冲池容量
}

//GetBufferPoolManager返回页面ID所对应的独立缓冲池指针，在这里，通过对页面ID取余的方式将页面ID映射至对应的缓冲池。
BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // 让BufferPoolManager负责处理给定的页面id。您可以在其他方法中使用此方法。
  return instances_[page_id % num_instances_];
```

```c++
 private:  //并行缓冲池成员

  std::vector<BufferPoolManager *> instances_;  //用于存储多个独立的缓冲池
  //std::vector<std::shared_ptr<BufferPoolManager>> instances_;
  size_t start_idx_{0};     
  size_t pool_size_;        //记录各缓冲池的容量
  size_t num_instances_;    //独立缓冲池的个数
  
};
```

并行缓冲池的成员如上，

- `instances_`用于存储多个独立的缓冲池
- `pool_size_`记录各缓冲池的容量
- num_instances_`为独立缓冲池的个数`
- `start_idx`见下文介绍。

## parallel_buffer_pool_manager.cpp

### parallel_buffer_pool_manager的构造/析构函数

```c++
ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  num_instances_ = num_instances;
  pool_size_ = pool_size;
  start_idx_ = 0;
  // resize以后内部全初始化为0，push_back后不是从索引0开始，而是直接添加到索引num_instances + 1
  //instances_.resize(num_instances);  
  for (size_t i = 0; i < num_instances; i++) {
    //智能指针
    //instances_[i] = std::make_shared<BufferPoolManagerInstance>(pool_size, num_instances, i, disk_manager, log_manager);
    BufferPoolManager *tmp = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    instances_.push_back(tmp);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete (instances_[i]);
  }
}
```

### 功能函数

下述函数仅需调用对应独立缓冲池的方法即可。

值得注意的是，由于在缓冲池中存放的为缓冲池实现类的基类指针，**因此所调用函数的应为缓冲池实现类的基类对应的虚函数。**

并且，由于`ParallelBufferPoolManager`和`BufferPoolManagerInstance`为兄弟关系，因此`ParallelBufferPoolManager`不能直接调用`BufferPoolManagerInstance`对应的`Imp`函数，因此直接在`ParallelBufferPoolManager`中存放`BufferPoolManagerInstance`指针也是不可行的。

(即在ParallelBufferPoolManager中只能存放BufferPoolManager的指针)

```c++
Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  //从负责的BufferPoolManagerInstance获取page_id的页面
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  //从负责的BufferPoolManagerInstance中取消固定page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // 从负责的BufferPoolManagerInstance刷新page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->FlushPage(page_id);
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // 从负责的BufferPoolManagerInstance中删除page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // 刷新所有BufferPoolManager实例中的所有页面
  for (size_t i = 0; i < num_instances_; i++) {
    instances_[i]->FlushAllPages();
  }
}
```

#### buffer_pool_manager

```c++
/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager {
 public:
  enum class CallbackType { BEFORE, AFTER };
  using bufferpool_callback_fn = void (*)(enum CallbackType, const page_id_t page_id);

  BufferPoolManager() = default;
  /**
   * Destroys an existing BufferPoolManager.
   */
  virtual ~BufferPoolManager() = default;

  /** Grading function. Do not modify! */
  auto FetchPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) -> Page * {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto *result = FetchPgImp(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto UnpinPage(page_id_t page_id, bool is_dirty, bufferpool_callback_fn callback = nullptr) -> bool {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = UnpinPgImp(page_id, is_dirty);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto FlushPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) -> bool {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = FlushPgImp(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto NewPage(page_id_t *page_id, bufferpool_callback_fn callback = nullptr) -> Page * {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    auto *result = NewPgImp(page_id);
    GradingCallback(callback, CallbackType::AFTER, *page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto DeletePage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) -> bool {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = DeletePgImp(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  void FlushAllPages(bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    FlushAllPgsImp();
    GradingCallback(callback, CallbackType::AFTER, INVALID_PAGE_ID);
  }

  /** @return size of the buffer pool */
  virtual auto GetPoolSize() -> size_t = 0;

 protected:
  /**
   * Grading function. Do not modify!
   * Invokes the callback function if it is not null.
   * @param callback callback function to be invoked
   * @param callback_type BEFORE or AFTER
   * @param page_id the page id to invoke the callback with
   */
  void GradingCallback(bufferpool_callback_fn callback, CallbackType callback_type, page_id_t page_id) {
    if (callback != nullptr) {
      callback(callback_type, page_id);
    }
  }

  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  virtual auto FetchPgImp(page_id_t page_id) -> Page * = 0;

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true otherwise
   */
  virtual auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool = 0;

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  virtual auto FlushPgImp(page_id_t page_id) -> bool = 0;

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  virtual auto NewPgImp(page_id_t *page_id) -> Page * = 0;

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  virtual auto DeletePgImp(page_id_t page_id) -> bool = 0;

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  virtual void FlushAllPgsImp() = 0;
};
```

### NewPgImp

在这里，为了使得各独立缓冲池的负载均衡，采用轮转方法选取分配物理页面时使用的缓冲池，在这里具体的规则如下：

- 从`start_idx_`开始遍历各独立缓冲池，
- 如存在，则调用`NewPage`成功的页面，
- 否则返回该页面并将`start_idx`指向该页面的下一个页面；
- 如全部缓冲池调用`NewPage`均失败，则返回空指针，并递增`start_idx`。

```c++
Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
    Page *ret;
    for (size_t i = 0; i < num_instances_; i++) {
        size_t idx = (start_idx_ + i) % num_instances_;
        if ((ret = instances_[idx]->NewPage()(page_id)) != nullptr) {   //如果创建新页面成功
            start_idx_ = (*page_id + 1) % num_instances_;               //下一次开始的索引为本页面的下一个索引
            return ret;                                                 //返回创建的新页面
        }
    }
    start_idx_++;   //如果遍历结束仍然没有创建成功，起始索引+1，即返回原始位置
    return nullptr; //返回nullptr
}
```

