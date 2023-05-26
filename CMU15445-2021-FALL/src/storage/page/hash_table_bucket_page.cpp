//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
//扫描bucket并收集具有匹配密钥的值
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


/*
Insert向桶插入键值对
遍历槽寻找可插入的地方
从小到大遍历所有槽
若槽可读或未访问，确定slot_idx
检测该槽是否被访问，若未访问，退出循环
若该槽已访问却可读，但已存在相应键值对，返回false
否则在array_中对应的数组中插入键值对。
*/

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


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx)) {
      RemoveAt(bucket_idx);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
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

/*
    index = bucket_index / 8
    offset = bucket_idx % 8
    假设bucket_idx % 8 为3
*/

template <typename KeyType, typename ValueType, typename KeyComparator>  // 设置可读
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {          // 0000 0000 |= 0000 1000 = 0000 1000
  readable_[bucket_idx / 8] |= 1 << (bucket_idx % 8);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {          // 设置已访问
  occupied_[bucket_idx / 8] |= 1 << (bucket_idx % 8);                    // 0000 0000 |= 0000 1000  = 0000 1000  假设bucket_idx % 8 为3
}

//拆除bucket_idx处的KV对
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // char 类型数组,每个单位 8 bit,能标记8个位置是否有键值对存在, bucket_idx / 8 
  // 找到应该修改的字节位置
  // bucket_idx % 8 找出在该字节的对应 bit 位 pos_
  // 构建一个 8 位长度的,除该 pos_ 位为 0 外,其他全为 1 的Byte ,例如: 11110111
  // 然后与相应字节取按位与操作,则实现清除该位置的 1 的操作,而其他位置保持不变
  readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));                // 0000 1000 &= 1111 0111 = 0000 0000  删除键值对， 设置为未读
}                                                                       //无需删除arry_里的键值对，等插入时自然会覆盖

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {    //只要occupied上有数据则必不为 0000 0000
  return (occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;    //0000 1000 & 0000 1000 = 0000 1000  已访问
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return (readable_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;    //0000 1000 & 0000 1000 = 0000 1000 可读
}




//打印存储桶的占用信息
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
