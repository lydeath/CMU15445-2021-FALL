//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);
  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param num_instances total number of BPIs in parallel BPM
   * @param instance_index index of this BPI in the parallel BPM
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                            DiskManager *disk_manager, LogManager *log_manager = nullptr);

  /**
   * Destroys an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;

  /** @return size of the buffer pool */
  size_t GetPoolSize() override { return pool_size_; }

  /** @return pointer to all the pages in the buffer pool */
  Page *GetPages() { return pages_; }

 protected:
  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  Page *FetchPgImp(page_id_t page_id) override;

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true otherwise
   */
  bool UnpinPgImp(page_id_t page_id, bool is_dirty) override;

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  bool FlushPgImp(page_id_t page_id) override;

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  Page *NewPgImp(page_id_t *page_id) override;

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  bool DeletePgImp(page_id_t page_id) override;

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override;

  /**
   * Allocate a page on disk.∂
   * @return the id of the allocated page
   */
  page_id_t AllocatePage();

  /**
   * Deallocate a page on disk.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  /**
   * Validate that the page_id being used is accessible to this BPI. This can be used in all of the functions to
   * validate input data and ensure that a parallel BPM is routing requests to the correct BPI
   * @param page_id
   * 验证此BPI是否可以访问正在使用的page_id。这可以用于以下所有功能
   * 验证输入数据，并确保并行BPM将请求路由到正确的BPI
   */
  void ValidatePageId(page_id_t page_id) const;

  // 获取页框，进入函数前需加锁
  frame_id_t GetFrame();

  static const frame_id_t NUMLL_FRAME = -1;

  /** Number of pages in the buffer pool. */\
  //缓冲池中的页数。
  const size_t pool_size_;

  /** How many instances are in the parallel BPM (if present, otherwise just 1 BPI) */
  //并行BPM中有多少实例（除非存在，否则只有1个BPI）
  const uint32_t num_instances_ = 1;

  /** Index of this BPI in the parallel BPM (if present, otherwise just 0) */
  //并行BPM中此BPI的索引（除非存在，否则仅为0）
  const uint32_t instance_index_ = 0;

  /** Each BPI maintains its own counter for page_ids to hand out, must ensure they mod back to its instance_index_ */
  //每个BPI都为要分发的page_id维护自己的计数器，必须确保它们变回其instance_index_
  std::atomic<page_id_t> next_page_id_ = instance_index_;

  /** Array of buffer pool pages. */
  //pages_为缓冲池中的实际容器页面槽位数组，用于存放从磁盘中读入的页面，并供DBMS访问
  Page *pages_;

  /** Pointer to the disk manager. */
  //disk_manager_为磁盘管理器，提供从磁盘读入页面及写入页面的接口
  DiskManager *disk_manager_ __attribute__((__unused__));

  /** Pointer to the log manager. */
  //log_manager_为日志管理器，本实验中不用考虑该组件
  LogManager *log_manager_ __attribute__((__unused__));

  /** Page table for keeping track of buffer pool pages. */
  //page_table_用于保存磁盘页面IDpage_id和槽位IDframe_id_t的映射
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /** Replacer to find unpinned pages for replacement. */
  //raplacer_用于选取所需驱逐的页面
  Replacer *replacer_;

  /** List of free pages. */
  //保存缓冲池中的空闲槽位ID
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;
};
}  // namespace bustub
//在这里，区分page_id和frame_id_t是完成本实验的关键。
