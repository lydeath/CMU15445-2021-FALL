概述

第二个编程项目是为BusTub DBMS实现一个磁盘支持的哈希表。您的哈希表负责快速的数据检索，而不必搜索数据库表中的每一条记录。

您将需要使用可扩展哈希哈希方案来实现哈希表。

该索引包括一个目录页，该目录页包含指向存储桶页的指针。该表将通过项目#1中的缓冲池访问页面。该表包含一个目录页，用于存储表和存储桶的所有元数据。您的哈希表需要支持对满/空存储桶进行存储桶拆分/合并，并支持在全局深度必须更改时进行目录扩展/收缩。

您需要在哈希表实现中完成以下任务：

- 页面布局

- 可扩展哈希实现

- 并发控制

# TASK #1 - PAGE LAYOUTS

您的哈希表旨在通过DBMS的BufferPoolManager进行访问。这意味着您无法分配内存来存储信息。所有内容都必须存储在磁盘页中，以便可以从DiskManager读取/写入。如果您创建了一个哈希表，将其页面写入磁盘，然后重新启动DBMS，那么您应该能够在重新启动后从磁盘加载回哈希表。

为了支持在页面顶部读取/写入哈希表桶，您将实现两个Page类来存储哈希表的数据。这是为了教您如何将BufferPoolManager中的内存分配为页面。

## hash_table_directory_page.h

```c++
 private:
  page_id_t page_id_;
  lsn_t lsn_;
  uint32_t global_depth_{0};
  uint8_t local_depths_[DIRECTORY_ARRAY_SIZE];
  page_id_t bucket_page_ids_[DIRECTORY_ARRAY_SIZE];
};
```

该页面类作为哈希表的目录页面，保存哈希表中使用的所有元数据，包括

- page_id_为该页面的页面ID，
- lsn_ 为日志序列号
- global_depth_为哈希表的全局深度
- local_depths_为哈希表的局部深度
- bucket_page_ids_为各目录项所指向的桶的页面ID。

## hash_table_directory_page.cpp

### GetGlobalDepthMask()

GetGlobalDepthMask()通过位运算返回用于计算全局深度低位的掩码

```c++
uint32_t HashTableDirectoryPage::GetGlobalDepthMask() { 
    return (1U << global_depth_) - 1;   //1U 代表无符号整数  GD 为 1  1<<1  return 2-1=1
    //      2^global_depth - 1                          //GD 为 2  1<<2  return 8-1=7
}														//8 = 1000; 7 = 0111
```

### GetSplitImageIndex()

GetSplitImageIndex()通过原bucket_id得到分裂后新的bucket_id

你现在可能还不理解这个东西在干什么, 他的作用是获取兄弟bucket 的bucket_idx(也就是所谓的splitImage), 也就是说, 我们要将传入的bucket_idx的local_depth的最高位取反后返回 
GetSplitImageIndex通过原bucket_id得到分裂后新的bucket_id,比如001 and 101原来都映射到 01 这个物理页，这个物理页分裂后变成001 and 101；
对于001来说101是新分裂出的页，对于101正好相反。

那么这个函数应该实现一个翻转最高位为输出编号的功能。

GetSplitImageIndex(101) = 001; 
GetSplitImageIndex(001) = 101
GetLocalHighBit() 用于取这个最高位

假设只有两个桶 0 1，深度皆为1，则0 ^ (1<<(1-1)) = 1
假设桶11深度为1,，则其实际上用到的位为1，对应的桶即为0  11 ^ 1 & 1 = 0
但实际上返回10也无所谓，因为二者深度相等才进行合并操作
当只有一个桶时返回本身

```C++
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {  // 得到与该桶对应的桶，即将该桶最高位置反
    uint32_t local_depth = GetLocalDepth(bucket_idx);			
    uint32_t local_mask = GetLocalDepthMask(bucket_idx);		//011
    if (local_depth == 0) {
      return 0;
    }
    return (bucket_idx ^ (1 << (local_depth - 1))) & local_mask;
```

^为异或符号，即最高位取反

1 << (local_depth - 1) 达到最高位 1000

然后按位异或 ：

​		按位异或为1时，如果最高位为1 则为 0； 如果最高位为0 则为1；	按位取反

​		按位异或为0时，如果为1 则为1；如果为0 则为0；按位不变

最后按位与 local_mask	若localdepth为3， 则localdepthmast为111

### GetLocalHighBit()

获取与bucket的本地深度相对应的高位。
*这与bucket索引本身不同。这种方法有助于找到桶的对或“分割图像”

```C++
uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) {
  size_t tmp = global_depth_ - local_depths_[bucket_idx];
  return bucket_idx >> tmp << tmp;  //GD 为 2， LD 为 2 最高位为 00
}         
```

### 功能函数

```c++
//增加目录的全局深度
void HashTableDirectoryPage::IncrGlobalDepth() { 
    global_depth_++; 
}

//减少目录的全局深度
void HashTableDirectoryPage::DecrGlobalDepth() { 
    global_depth_--; 
}

//使用目录索引查找bucket页面
page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) { 
    return bucket_page_ids_[bucket_idx]; 
}

//使用bucket索引和page_id更新目录索引
void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}
//当前目录大小
uint32_t HashTableDirectoryPage::Size() {
    return 1 << global_depth_;   //2^global_depth_ 就是逻辑页的个数
}

//获得局部深度
uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) { 
    return local_depths_[bucket_idx]; 
}

//设置局部深度
void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

//局部深度增长
void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { 
    local_depths_[bucket_idx]++; 
}

//局部深度减弱
void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { 
    local_depths_[bucket_idx]--; 
}
```

### CanShrink()

CanShrink() 检查当前所有有效目录项的局部深度是否均小于全局深度，以判断是否可以进行表合并。

```c++
bool HashTableDirectoryPage::CanShrink() {  
  // 目录大小
  uint32_t bucket_num = 1 << global_depth_;  // 1 左移运算符  1 << 1 = 001 -> 0010 = 2^1
  for (uint32_t i = 0; i < bucket_num; i++) {              // 1 << 2 = 001 -> 0100 = 2^2
    if (local_depths_[i] == global_depth_) {                        //        1000 = 2^3
      return false;     
    }
  }
  return true;
}
```

### hash_table_bucket_page.h

```c++
   private:
   // For more on BUCKET_ARRAY_SIZE see storage/page/hash_table_page_defs.h
   char occupied_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
   // 0 if tombstone/brand new (never occupied), 1 otherwise.
   char readable_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
   // Do not add any members below array_, as they will overlap.
   MappingType array_[0];
```

该页面类用于存放哈希桶的键值与存储值对，以及桶的槽位状态数据。

- `occupied_`数组用于判断该页的某个位置是否曾经被访问过(一次访问, 永远为true)，即用于统计桶中的槽是否被使用过，当一个槽被插入键值对时，其对应的位被置为1，事实上，`occupied_`完全可以被一个`size`参数替代，但由于测试用例中需要检测对应的`occupied`值，因此在这里仍保留该数组；
- `readable_`数组用于标记桶中的槽是否被占用，当被占用时该值被置为1，否则置为0；
- `array_`是C++中一种弹性数组的写法，在这里只需知道它用于存储实际的键值对即可。

### hash_table_bucket_page.cpp

在这里，使用`char`类型存放两个状态数据数组，在实际使用应当按位提取对应的状态位。下面是使用位运算的状态数组读取和设置函数：

```c++
//拆除bucket_idx处的KV对
//将bucket_idx对应桶设为未读，无需删除array_中的键值对
//当Insert()时，会自动覆盖
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // char 类型数组,每个单位 8 bit,能标记8个位置是否有键值对存在, bucket_idx / 8 
  // 找到应该修改的字节位置
  // bucket_idx % 8 找出在该字节的对应 bit 位 pos_
  // 构建一个 8 位长度的,除该 pos_ 位为 0 外,其他全为 1 的Byte ,例如: 11110111
  // 然后与相应字节取按位与操作,则实现清除该位置的 1 的操作,而其他位置保持不变
  readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));                // 0000 1000 &= 1111 0111 = 0000 0000  删除键值对， 设置为未读
}                                                                       //无需删除arry_里的键值对，等插入时自然会覆盖

 
//查看是否访问过
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {    //只要occupied上有数据则必不为 0000 0000
  return (occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;    //0000 1000 & 0000 1000 = 0000 1000  已访问
}
 
//设置已访问
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {          // 设置已访问
  occupied_[bucket_idx / 8] |= 1 << (bucket_idx % 8);                    // 0000 0000 |= 0000 1000  = 0000 1000  假设bucket_idx % 8 为3
}
 
//查看是否可读
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return (readable_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;    //0000 1000 & 0000 1000 = 0000 1000 可读
}
 
 //设置可读性
template <typename KeyType, typename ValueType, typename KeyComparator>  // 设置可读
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {          // 0000 0000 |= 0000 1000 = 0000 1000
  readable_[bucket_idx / 8] |= 1 << (bucket_idx % 8);
}
```

对于对应索引的键值读取直接访问`array_`数组即可：

```c++
//查看键
template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

//查看值
template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}
```

### GetValue()

`GetValue`提取桶中槽的键为`key`的所有值

- 遍历所有桶中的槽
- 如果槽未被访问，跳出循环
- 如果槽已访问且可读并且键匹配，将键对应的值插入result数组

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
//GetValue提取桶中键为key的所有值，实现方法为遍历所有occupied_为1的位，
//并将键匹配的值插入result数组即可，//如至少找到了一个对应值，则返回真。在这里，可以看出
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool ret = false;  // 标志是否找到相应value值
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {  //如果bucket_idx没有被访问 则继续
      break;
    }
    //如果可读，并且键匹配， 插入result数组
    if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0) {
      result->push_back(array_[bucket_idx].second);  //array_ 存储真正的(key, value)键值对
      ret = true;
    }
  }
  return ret;
}
```

### Insert()

Insert向桶中的槽插入键值对

- 遍历槽寻找可插入的地方，从小到大遍历所有槽
- 若槽可读或未访问，确定slot_idx
- 检测该槽是否被访问，若未访问，退出循环
- 若该槽已访问却可读，但已存在相应键值对，返回false
- 若该槽未访问或已访问可读，则在array_中对应的数组中插入键值对。

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t slot_idx = 0;
  bool slot_found = false;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    //如果当前bucket_idx对应槽 slot尚未找到 与 （不可读 或 未访问）
    if (!slot_found && (!IsReadable(bucket_idx) || !IsOccupied(bucket_idx))) {
        slot_found = true;          //设置slot找到了
        slot_idx = bucket_idx;      //设置slot_idx
    }
    if (!IsOccupied(bucket_idx)) {  //如果未访问则退出遍历开始插入键值对
        break;
    }
    // 如果可读并且键值匹配，则已有相同元素，返回false
    if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx)) {
        return false;
    }
  }
    if (slot_found) {                            // 如果slot_found = true
    SetReadable(slot_idx);                       // 设置对应位置可读
    SetOccupied(slot_idx);                       // 设置对应位置以访问
    array_[slot_idx] = MappingType(key, value);  // 存入键值对
    return true;
    }
    return false;
}
```

### Remove()

`Remove`从桶中的槽删除对应的键值对，遍历所有槽即可。

- 遍历桶中的槽
- 如果槽未被访问，返回false
- 如果槽可读且对应键值对相同，删除对应键值对，返回true

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx)) {
      RemoveAt(bucket_idx);  //将bucket_idx对应桶设为未读，无需删除array_中的键值对
      return true;			 //当Insert()时，会自动覆盖
    }
  }
  return false;
}
```

### NumReadable()/IsFull()/IsEmpty()

`NumReadable()`返回槽中的键值对个数，遍历即可。

`IsFull()`和`IsEmpty()`直接复用`NumReadable()`实现。

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

// NumReadable() 返回桶中的键值对个数，遍历即可。IsFull() 和IsEmpty() 直接复用NumReadable() 实现。
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t ret = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      ret++;
    }
  }
  return ret;
}
```

### Page与上述两个页面类的转换

在本部分中，有难点且比较巧妙的地方在于理解上述两个页面类是如何与`Page`类型转换的。在这里，上述两个页面类并非为`Page`类的子类，在实际应用中通过`reinterpret_cast`将`Page`与两个页面类进行转换。在这里我们回顾一下`Page`的数据成员：

```c++
 77  private:
 78   /** Zeroes out the data that is held within the page. */
 79   inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }
 80 
 81   /** The actual data that is stored within a page. */
 82   char data_[PAGE_SIZE]{};
 83   /** The ID of this page. */
 84   page_id_t page_id_ = INVALID_PAGE_ID;
 85   /** The pin count of this page. */
 86   int pin_count_ = 0;
 87   /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
 88   bool is_dirty_ = false;
 89   /** Page latch. */
 90   ReaderWriterLatch rwlatch_;
 91 };
```

可以看出，`Page`中用于存放实际数据的`data_`数组位于数据成员的第一位，其在栈区固定分配一个页面的大小。因此，在`Page`与两个页面类强制转换时，通过两个页面类的指针的操作仅能影响到`data_`中的实际数据，而影响不到其它元数据。并且在内存管理器中始终是进行所占空间更大的通用页面`Page`的分配（实验中的`NewPage`），因此页面的容量总是足够的。

## TASK #2 - HASH TABLE IMPLEMENTATION/TASK #3 - CONCURRENCY CONTROL

在这两个部分中，我们需要实现一个线程安全的可扩展哈希表。在对可扩展哈希表的原理清楚后，将其实现并不困难，难点在于如何在降低锁粒度、提高并发性的情况下保证线程安全。

### extendible_hash_table.h

```c++
  page_id_t directory_page_id_;		
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;

  // Readers includes inserts and removes, writers are splits and merges
  ReaderWriterLatch table_latch_;
  HashFunction<KeyType> hash_fn_;
```

- directory_page_id_ 为目录页面ID
- buffer_pool_manager_ 为缓冲池
- comparator_ 为比较函数
- table_latch 为表锁
- hash_fn 为哈希函数

### ExtendibleHashTable()构造函数

由缓冲池管理器支持的可扩展哈希表的实现。
支持非唯一密钥。支持插入和删除。
当存储桶变满/变空时，表会动态增长/收缩。

在构造函数中，

- 为哈希表分配一个目录页面和桶页面，并设置目录页面的page_id成员。
- 将哈希表的首个目录项指向该桶。
- 最后，不要忘记调用UnpinPage并设置脏页向缓冲池告知页面的使用完毕。

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // LOG_DEBUG("BUCKET_ARRAY_SIZE = %ld", BUCKET_ARRAY_SIZE);

    //分配目录页面，强制转换后数据全落在data_上,不影响其他元数据
    HashTableDirectoryPage *dir_page =              //创建目录页，为目录页分配page_id
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
    dir_page->SetPageId(directory_page_id_);        //设置目录页面ID

    //分配桶页面
    page_id_t new_bucket_id;
    buffer_pool_manager->NewPage(&new_bucket_id);   // 申请第一个桶的页

    dir_page->SetBucketPageId(0, new_bucket_id);    //更新目录页的bucket_page_ids_(bucket_idx, bucket_page_id)

    //调用UnpinPage向缓冲池告知页面的使用完毕
  //auto UnpinPage(page_id_t page_id, bool is_dirty, bufferpool_callback_fn callback = nullptr) -> bool
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));  //取消固定并设置为脏页
  assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
```

### 功能函数

下面是一些用于提取目录页面、桶页面以及目录页面中的目录项的功能函数。

```c++
//Hash-将MurmurHash的64位哈希向下转换为32位的简单助手
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

//获得哈希键在目录页面的桶页面索引
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hashed_key = Hash(key);
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return mask & hashed_key;
}

//获取哈希键，通过从目录页上获得的桶页面索引获得对应的桶页面的桶ID
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(idx);
}

//从缓冲池管理器获取目录页。
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

//使用存储桶的page_id从缓冲池管理器中获取存储桶页面。
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}
```

### GetValue()

`GetValue`从哈希表中读取与键匹配的所有值结果，其通过哈希表的读锁保护目录页面，并使用桶的读锁保护桶页面。

- 先读取目录页面
- 为目录页上读锁
- 再通过目录页面和哈希键获取对应的桶页面
- 将桶页面强制转化为Page类型上读锁
- 最后调用桶页面的`GetValue`获取值结果。
- 将桶页面与目录页解锁
- 为目录页与桶页面取消固定

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {  
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();                      //获得目录页
  table_latch_.RLock();                                                         //上表读锁（表为目录页）
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);                        //获得桶ID
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);             //获得桶页面
  Page *p = reinterpret_cast<Page *>(bucket);                                   //将桶页面强转为Page
  p->RLatch();                                                                  //桶页面上读锁(页锁)
  bool ret = bucket->GetValue(key, comparator_, result);                        //获取对应值，填入result中，返回True
  p->RUnlatch();                                                                //桶页面解读锁(页层面)
  table_latch_.RUnlock();                                                       //解表读锁
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));  //取消固定
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));

  return ret;
}
```

### Insert()

`Insert`向哈希表插入键值对

这可能会导致桶的分裂和表的扩张，因此需要保证目录页面的读线程安全，一种比较简单的保证线程安全的方法为：在操作目录页面前对目录页面加读锁。但这种加锁方式使得`Insert`函数阻塞了整个哈希表，这严重影响了哈希表的并发性。可以注意到，表的扩张的发生频率并不高，对目录页面的操作属于读多写少的情况，因此可以使用乐观锁的方法优化并发性能，**其在`Insert`被调用时仅保持读锁，只在需要桶分裂时重新获得写锁。**

`Insert`函数的具体流程为：

- 获取目录页面和桶页面
- 在加全局读锁和桶写锁后检查桶是否已满
- 如已满则释放锁，并调用`UnpinPage`释放页面
- 然后调用`SplitInsert`实现桶分裂和插入
- 如当前桶未满，则直接向该桶页面插入键值对
- 释放锁和页面即可。

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    HashTableDirectoryPage *dir_page = FetchDirectoryPage();                    //获取目录页面
    table_latch_.RLock();                                                       //目录页面上读锁
    page_id_t bucket_page_id = KeyToPageId(key, dir_page);                      //通过从目录页上获得的桶页面索引获得对应的桶页面的桶ID
    HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);           //获取桶页面
    Page *p = reinterpret_cast<Page *>(bucket);                                 //为给桶页面添加锁强制转换
    p->WLatch();                                                                //桶页面添加写锁
    if (bucket->IsFull) {                                                       //如果桶已满，需要分裂
        p->WUnlatch();                                                          //桶页解写锁
        table_latch_.RUnlock();                                                 //目录页解读锁
        assert(buffer_pool_manager_->UnpinPage(bucket, true, nullptr));         //桶页面取消固定，设置为脏页
        assert(buffer_pool_manager_->UnpinPage(dir_page, true, nullptr));       //目录页面取消固定，设置为脏页
        return SplitInsert(transaction, key, value);                            //调用分裂插入函数
    }
    bool ret = bucket->Insert(key, value, comparator_);                         //如果桶无需分裂，直接插入键值对
    p->WUnlatch();                                                              //桶页面解除写锁
    table_latch_.RUnlock();                                                     //解锁
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr)); //目录页面取消固定
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));     //桶页面取消固定
    return ret;
}
```

### SplitInsert()

SplitInsert()使用可选的桶拆分执行插入。

如果页面在分割后仍然是满的，然后递归分割。这种情况极为罕见，但有可能。

- 获取目录页面并加全局写锁，在添加全局写锁后，其他所有线程均被阻塞了，因此可以放心的操作数据成员
- 不难注意到，在Insert中释放读锁和SplitInsert中释放写锁间存在空隙，其他线程可能在该空隙中被调度，从而改变桶页面或目录页面数据。因此，在这里需要重新在目录页面中获取哈希键所对应的桶页面（可能与Insert中判断已满的页面不是同一页面）
- 检查对应的桶页面是否已满
- 如桶页面仍然是满的，则分配新桶和提取原桶页面的元数据
- 由于桶分裂后仍所需插入的桶仍可能是满的，因此在这里进行循环以解决该问题
- 需要根据全局深度和桶页面的局部深度判断扩展表和分裂桶的策略。
- 当`global_depth == local_depth`时，需要进行表扩展和桶分裂
- `global_depth > local_depth`仅需进行桶分裂即可。
- 在完成桶分裂后，应当将原桶页面中的记录重新插入哈希表
- 由于记录的低`i-1`位仅与原桶页面和新桶页面对应，因此记录插入的桶页面仅可能为原桶页面和新桶页面两个选择。
- 在重新插入完记录后，释放新桶页面和原桶页面。
- 若当前键值对所插入的桶页面非空（被其他线程修改或桶分裂后结果），则直接插入键值对，并释放锁和页面，并将插入结果返回`Insert`。

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    HashTableDirectoryPage *dir_page = FetchBucketPage();                                                   // 重新获取目录页面
    table_latch_.WLock();
    while (true) {
        uint32_t bucket_idx = KeyToDirectoryIndex(ket, dir_page);                                           //获取目录索引
        page_id_t bucket_page_id = KeyToPageId(key, dir_page);                                              //通过目录索引获取桶页面ID
        HASH_TABLE_BUCKET_TYPE *bucket = FetchDirectoryPage(bucket_page_id);                                //获取桶页面ID

        if (bucket->IsFull) {                                                                               //如果桶仍是满的，分裂
            uint32_t global_depth = dir_page->GetGlobalDepth();                                             //获取全局深度
            uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);                                     //获取要分裂桶的局部深度
            page_id_t new_bucket_id = 0;
            HASH_TABLE_BUCKET_TYPE *new_bucket =                                                            //获取一个新物理页面来存放分裂后的新桶
                reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_id));
            assert(new_bucket != nullptr);

            if (global_depth == local_depth) {                                                              //如果局部深度等于全局深度，分裂目录
                uint32_t bucket_num = 1 << global_depth;                                                    //获取桶的总数
                for (uint32_t i = 0; i < bucket_num; i++) {                                                 
                    dir_page->SetBucketPageId(i + bucket_num, dir_page->GetBucketPageId(i));                //设置分裂目录页面
                    dir_page->SetLocalDepth(i + bucket_num, dir_page->GetLocalDepth(i));                    //设置分裂目录里桶的局部深度
                }
                dir_page->IncrGlobalDepth();                                                                //分裂后，全局深度加一
                dir_page->SetBucketPageId(bucket_idx + bucket_num, new_bucket_id);                          //更新哈希表指向的分裂桶
                dir_page->IncrLocalDepth(bucket_idx);                                                       //添加原桶局部深度
                dir_page->IncrLocalDepth(bucket_idx + bucket_num);                                          //添加分裂桶局部深度
                global_depth++;                                                                             //全局深度加一
            } else {                //如果局部深度小于全局深度，则分裂桶，无需分裂目录
                /*  i = GD, j = LD
                   兄弟目录项中的最顶端（位表示最小）目录项为低j位不变、其余位为0的目录项；
                   相邻两个目录项的哈希键相差    step = 1<<j
                   分裂后相邻两个兄弟目录项的哈希键相差 step*2
                   兄弟目录项的总数为1<<(i - j)。
                   需要更改的项为 1<<(i - j - 1)
                */
                // 此处为old_mask，为111  new_mask为1111
                uint32_t mask = (1 << local_depth) - 1;     // 2^1 - 1 = 1 -> 0001

、              // 初始ID
                // 0111 & 1111 = 0111，即为分裂前桶ID，即base_idx
                uint32_t base_idx = mask & bucket_idx;

                // 需要更改的数量，例如GD=2, LD=1 recordes_num = 1; GD=3, LD=2 recordes_num = 1; GD=3, LD=1 recordes_num = 2
                uint32_t records_num = 1 << (global_depth - local_depth - 1);  // 2 ^ (2 - 1 - 1)

                // 间隔步伐 LD = 1，step = 2;  LD = 2，step = 4
                uint32_t step = (1 << local_depth);  // 2^local_depth
                uint32_t idx = base_idx;

                // 首先遍历一遍目录，将仍指向旧桶的位置深度加一
                for (uint32_t i = 0; i < records_num; i++) {
                    dir_page->IncrLocalDepth(idx);  // 目录对应原桶局部深度加一
                    idx += step * 2;
                }

                // 而后依据是否影响全局深度，对各位置进行操作
                idx = base_idx + step;
                for (uint32_t i = 0; i < records_num; i++) {  // 目录后半局部深度加一，更新目录指向新桶，并将深度加一
                    dir_page->SetBucketPageId(idx, new_bucket_id);
                    dir_page->IncrLocalDepth(idx);
                    idx += step * 2;
                }
            }
            /*
                在完成桶分裂后，应当将原桶页面中的记录重新插入哈希表，由于记录的低i-1位仅与原桶页面和新桶页面对应，
                因此记录插入的桶页面仅可能为原桶页面和新桶页面两个选择。在重新插入完记录后，释放新桶页面和原桶页面。
            */
            for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
                KeyType j_key = bucket->KeyAt(i);                                   //获取桶内槽的哈希键
                ValueType j_value = bucket->ValueAt(i);                             //获取槽的值
                bucket->RemoveAt(i);                                                //删除桶内槽的键值对
                if (KeyToPageId(j_key, dir_page) == bucket_page_id) {               //如果哈希键等于原桶
                    bucket->Insert(j_key, j_value, comparator_);                    //插入原桶槽
                } else {                                                            //如果哈希键等于分裂桶
                    new_bucket->Insert(j_key, j_value, comparator_);                //插入分裂桶槽
                }
            }
            assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));  // 原桶页面取消确定
            assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));   // 新桶页面取消固定
        } else {
            bool ret = bucket->Insert(key, value, comparator_);                         // 直接插入
            table_latch_.WUnlock();                                                     // 目录页解写锁
            assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr)); // 目录页取消固定
            assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));     // 桶页取消固定
            return ret;
        }
    }
    return false;
}
```

### Remove()

`Remove`从哈希表中删除对应的键值对，其优化思想与`Insert`相同

- 由于桶的合并并不频繁，因此在删除键值对时仅获取全局读锁，只在需要合并桶时获取全局写锁
- 当删除后桶为空且目录项的局部深度不为零时，释放读锁并调用`Merge`尝试合并页面
- 随后释放锁和页面并返回

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();                      //获得目录页
  table_latch_.RLock();                                                         //上读锁
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);                     //获得目录页存的桶页索引
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);                        //获得目录页存的桶页ID
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);             //获得桶页面
  Page *p = reinterpret_cast<Page *>(bucket);
  p->WLatch();
  bool ret = bucket->Remove(key, value, comparator_);                           //删除对应键值对
  p->WUnlatch();
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {          //如果桶删除后为空且局部深度不为0
    table_latch_.RUnlock();                                                     //解锁读锁
    this->Merge(transaction, key, value);                                       //合并
  } else {                                                                      //如果桶不为空，直接解除读锁
    table_latch_.RUnlock();                                         
  }
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));       //桶页面取消固定,设置为脏页
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));   //目录页面取消固定,设置为脏页
  return ret;
```

### Merge()

merge()函数合并空桶

- 在`Merge`函数获取写锁后，需要重新判断是否满足合并条件，以防止在释放锁的空隙时页面被更改
- 在合并被执行时，需要判断当前目录页面是否可以收缩
- 如可以收缩，则仅需在此递减全局深度即可完成收缩
- 最后释放页面和写锁。

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();                                          //获得最新目录页面     
  table_latch_.WLock();                                                                             //上写锁
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);                                         //从目录页面获取桶ID
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);                                 //从目录页面获取桶索引
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);                                 //获得最新被删除键值对的桶页面
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {                              //若桶为空且局部深度不为0
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);                                     //获取局部深度
    uint32_t global_depth = dir_page->GetGlobalDepth();                                             //获取全局深度
    // 如何找到要合并的bucket？
    // 答：合并后，指向Merged Bucket的记录，
    // 具有相同的低（local_depth-1）位
    // 因此，反转低local_depth可以获得要合并的bucket的idx点
    uint32_t merged_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));                             //获取目录存的兄弟桶索引
    page_id_t merged_page_id = dir_page->GetBucketPageId(merged_bucket_idx);                        //获取要合并的bucket的页面ID
    HASH_TABLE_BUCKET_TYPE *merged_bucket = FetchBucketPage(merged_page_id);                        //获取最新的要合并的bucket页面
    if (dir_page->GetLocalDepth(merged_bucket_idx) == local_depth && merged_bucket->IsEmpty()) {    //如果合并的Bucket页面局部深度等于删除Bucket页面的局部深度，且为空桶
      local_depth--;     //局部深度-1，为计算分裂前的索引，不影响目录中保存的局部深度
      // 此处为过去的掩码，例如111，原掩码为1111
      uint32_t mask = (1 << local_depth) - 1;  
      // 0111 & 1111 = 0111
      uint32_t idx = mask & bucket_idx;
      // 兄弟目录项的总数为1 << (i - j)。
      uint32_t records_num = 1 << (global_depth - local_depth);
      // 相邻两个目录项的哈希键相差 step = 1 << j 
      // 分裂后相邻两个兄弟目录项的哈希键相差 step * 2
      uint32_t step = (1 << local_depth);

      for (uint32_t i = 0; i < records_num; i++) {                  //更新
        dir_page->SetBucketPageId(idx, bucket_page_id);             //将目录保存的两个页面的页面ID设置为一样的
        dir_page->DecrLocalDepth(idx);
        idx += step;
      }
      buffer_pool_manager_->DeletePage(merged_page_id);             //删除被合并的页面
    }
    if (dir_page->CanShrink()) {        //判断目录页面是否收缩
      dir_page->DecrGlobalDepth();
    }
    assert(buffer_pool_manager_->UnpinPage(merged_page_id, true, nullptr));     //合并页面取消确定
  }
  table_latch_.WUnlock();                                                       //解读锁
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));   //目录页面取消固定
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));       //删除页面取消固定
}
```



