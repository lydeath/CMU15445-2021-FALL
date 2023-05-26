//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/*                           PAGE
     private:
     Zeroes out the data that is held within the page. 
    // 将页面中保存的数据归零。
    inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

    // Page是缓冲池中的页面容器

    The actual data that is stored within a page. 
    data_保存对应磁盘页面的实际数据
    char data_[PAGE_SIZE]{};

    The ID of this page. 
    // page_id_保存该页面在磁盘管理器中的页面ID
    page_id_t page_id_ = INVALID_PAGE_ID;

     The pin count of this page. 
    pin_count_保存DBMS中正使用该页面的用户数目
    int pin_count_ = 0;

    True if the page is dirty, i.e. it is different from its corresponding page on disk. 
    is_dirty_保存该页面自磁盘读入或写回后是否被修改
    bool is_dirty_ = false;
     Page latch. 
    ReaderWriterLatch rwlatch_;

*/


#include "buffer/buffer_pool_manager_instance.h"

#include <cstdio>
#include <iostream>
#include "common/macros.h"
namespace bustub {

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

  // We allocate a consecutive memory space for the buffer pool.
  // 我们为缓冲池分配一个连续的内存空间。
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
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

// FlushPgImp用于显式地将缓冲池页面写回磁盘。
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

//  FlushAllPgsImp将缓冲池内的所有页面写回磁盘。
  void BufferPoolManagerInstance::FlushAllPgsImp() { 
    std::lock_guard<std::mutex> lock(latch_);     // 上锁
    for (auto id : page_table_) {                 // 遍历page_table_以获得缓冲池内的<页面ID - 槽位ID>对
        pages_[id.second].is_dirty_ = false;      // 将缓冲池内所有页面的is_dirty_置为false
        disk_manager_->WritePage(id.first, pages_[id.second].GetData());    //使用WritePage将该页面的实际数据data_写回磁盘
    }
  }

//GetFrame()将 获取frame_id，进入函数前需加锁
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
}
    
 // NewPgImp在磁盘中分配新的物理页面，将其添加至缓冲池，并返回指向缓冲池页面Page的指针。
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

    return &pages_[new_frame_id];                   // 返回指向缓冲池页面Page的指针
}

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
 
//UnpinPgImp的功能为提供用户向缓冲池通知页面使用完毕的接口，
//用户需声明使用完毕页面的页面ID以及使用过程中是否对该页面进行修改。
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

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
                                                        //分配的页面修改回此BPI
}

}  // namespace bustub
