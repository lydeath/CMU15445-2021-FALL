//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include <iostream>

// 不难看出，上述缓冲池实现的问题在于锁的粒度过大，其在进行任何一项操作时都将整个缓冲池锁住，因此几乎不存在并行性。在这里，
// 并行缓冲池的思想是分配多个独立的缓冲池，并将不同的页面ID映射至各自的缓冲池中，从而减少整体缓冲池的锁粒度，以增加并行性。
namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
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

//GetPoolSize应返回全部缓冲池的容量，即独立缓冲池个数乘以缓冲池容量。
size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances  获取所有BufferPoolManager实例的大小
  return num_instances_ * pool_size_;  // 缓冲池个数 * 缓冲池容量
}

//GetBufferPoolManager返回页面ID所对应的独立缓冲池指针，在这里，通过对页面ID取余的方式将页面ID映射至对应的缓冲池。
BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  // 让BufferPoolManager负责处理给定的页面id。您可以在其他方法中使用此方法。
  return instances_[page_id % num_instances_];
}


//************************************************************************************************************
Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  //从负责的BufferPoolManagerInstance获取page_id的页面
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  //从负责的BufferPoolManagerInstance中取消固定page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  // 从负责的BufferPoolManagerInstance刷新page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->FlushPage(page_id);
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  // 从负责的BufferPoolManagerInstance中删除page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  // 刷新所有BufferPoolManager实例中的所有页面
  for (size_t i = 0; i < num_instances_; i++) {
    instances_[i]->FlushAllPages();
  }
}

// 上述函数仅需调用对应独立缓冲池的方法即可。值得注意的是，由于在缓冲池中存放的为缓冲池实现类的基类指针，因此所调用
// 函数的应为缓冲池实现类的基类对应的虚函数。并且，由于ParallelBufferPoolManager和BufferPoolManagerInstance为兄弟关系
// ，因此ParallelBufferPoolManager不能直接调用BufferPoolManagerInstance对应的Imp函数，因此直接在ParallelBufferPoolMan
// ager中存放BufferPoolManagerInstance指针也是不可行的(即在ParallelBufferPoolManager中只能存放BufferPoolManager的指针)
// 
//                          ParallelBufferPoolManager::BufferPoolManager
//                          BufferPoolManagerInstance::BufferPoolManager
// 
// 
//***************************************************************************************************************************

//在这里，为了使得各独立缓冲池的负载均衡，采用轮转方法选取分配物理页面时使用的缓冲池，在这里具体的规则如下：
//从start_idx_开始遍历各独立缓冲池，如存在调用NewPage成功的页面，则返回该页面并将start_idx指向该页面的下一个页面；
//如全部缓冲池调用NewPage均失败，则返回空指针，并递增start_idx。 
// 

  // 创建新页面。我们将以循环方式从底层请求页面分配
  // BufferPoolManager实例
  //  1.   从BPMI的起始索引中，调用NewPageImpl，直到
  //        1）成功，然后返回
  //        2）循环到起始索引并返回nullptr
  //  2.   每次调用此函数时，调整起始索引（mod实例数）以在不同的BPMI处开始搜索
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


}  // namespace bustub
