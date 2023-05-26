//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }


uint32_t HashTableDirectoryPage::GetGlobalDepth() {
    return global_depth_; 
}

//  GetGlobalDepthMask通过位运算返回用于计算全局深度低位的掩码
uint32_t HashTableDirectoryPage::GetGlobalDepthMask() { 
    return (1U << global_depth_) - 1;   //1U 代表无符号整数  GD 为 1  1<<1  return 1  1
    //      2^global_depth - 1                          //   GD 为 2  1<<2  return 3 11
}


//你现在可能还不理解这个东西在干什么, 他的作用是获取兄弟bucket
// 的bucket_idx(也就是所谓的splitImage), 也就是说,
//   我们要将传入的bucket_idx的local_depth的最高位取反后返回 
// 
// 
//GetSplitImageIndex通过原bucket_id得到分裂后新的bucket_id,比如001 
//and 101原来都映射到 01 这个物理页，这个物理页分裂后变成001 and 101；
//对于001来说101是新分裂出的页，对于101正好相反。那么这个函数应该实现
//一个翻转最高位为输出编号的功能。GetSplitImageIndex(101) = 001; 
//GetSplitImageIndex(001) = 101
//GetLocalHighBit() 用于取这个最高位
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {  // 得到与该桶对应的桶，即将该桶最高位置反
    uint32_t local_depth = GetLocalDepth(bucket_idx);
    uint32_t local_mask = GetLocalDepthMask(bucket_idx);
    if (local_depth == 0) {
      return 0;
    }
    return (bucket_idx ^ (1 << (local_depth - 1))) & local_mask;
    // 假设只有两个桶 0 1，深度皆为1，则0 ^ (1<<(1-1)) = 1
    // 假设桶11深度为1,，则其实际上用到的位为1，对应的桶即为0  11 ^ 1 & 1 = 0
    // 但实际上返回10也无所谓，因为二者深度相等才进行合并操作
    // 当只有一个桶时返回本身
}

/**
 *获取与bucket的本地深度相对应的高位。
 *这与bucket索引本身不同。这种方法
 *有助于找到桶的对或“分割图像”。
 *
 *@param bucket_idx要查找的bucket索引
 *@返回桶局部深度对应的高位
 */
uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) {
  size_t tmp = global_depth_ - local_depths_[bucket_idx];
  return bucket_idx >> tmp << tmp;  //GD 为 2， LD 为 2 最高位为 00
}                                   //GD 为 3， LD 为 2 最高位为 10

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

//判断是否可以收缩全局深度，即判断是否所有局部深度都小于全局深度
//CanShrink() 检查当前所有有效目录项的局部深度是否均小于全局深度，以判断是否可以进行表合并。
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

uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) { 
    return local_depths_[bucket_idx]; 
}

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { 
    local_depths_[bucket_idx]++; 
}

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { 
    local_depths_[bucket_idx]--; 
}



/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.        local depth <= global depth                 局部深度要小于全局深度
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.   每个bucket都有准确的2^(GD - LD)个指针指向它
 * (3) The LD is the same at each index with the same bucket_page_id    LD在具有相同bucket_page_id的每个索引处是相同的
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_count, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
