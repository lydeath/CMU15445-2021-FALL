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
//ɨ��bucket���ռ�����ƥ����Կ��ֵ
//GetValue��ȡͰ�м�Ϊkey������ֵ��ʵ�ַ���Ϊ��������occupied_Ϊ1��λ��
//������ƥ���ֵ����result���鼴�ɣ�//�������ҵ���һ����Ӧֵ���򷵻��档��������Կ���
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool ret = false;  // ��־�Ƿ��ҵ���Ӧvalueֵ
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {  //���bucket_idxû�б����� �����
      break;
    }
    //����ɶ������Ҽ�ƥ�䣬 ����result����
    if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0) {
      result->push_back(array_[bucket_idx].second);  //array_ �洢������(key, value)��ֵ��
      ret = true;
    }
  }
  return ret;
}


/*
Insert��Ͱ�����ֵ��
������Ѱ�ҿɲ���ĵط�
��С����������в�
���ۿɶ���δ���ʣ�ȷ��slot_idx
���ò��Ƿ񱻷��ʣ���δ���ʣ��˳�ѭ��
���ò��ѷ���ȴ�ɶ������Ѵ�����Ӧ��ֵ�ԣ�����false
������array_�ж�Ӧ�������в����ֵ�ԡ�
*/

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t slot_idx = 0;
  bool slot_found = false;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    //�����ǰbucket_idx��Ӧ�� slot��δ�ҵ� �� �����ɶ� �� δ���ʣ�
    if (!slot_found && (!IsReadable(bucket_idx) || !IsOccupied(bucket_idx))) {
        slot_found = true;          //����slot�ҵ���
        slot_idx = bucket_idx;      //����slot_idx
    }
    if (!IsOccupied(bucket_idx)) {  //���δ�������˳�������ʼ�����ֵ��
        break;
    }
    // ����ɶ����Ҽ�ֵƥ�䣬��������ͬԪ�أ�����false
    if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx)) {
        return false;
    }
  }
    if (slot_found) {                            // ���slot_found = true
    SetReadable(slot_idx);                       // ���ö�Ӧλ�ÿɶ�
    SetOccupied(slot_idx);                       // ���ö�Ӧλ���Է���
    array_[slot_idx] = MappingType(key, value);  // �����ֵ��
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

// NumReadable() ����Ͱ�еļ�ֵ�Ը������������ɡ�IsFull() ��IsEmpty() ֱ�Ӹ���NumReadable() ʵ�֡�
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
    ����bucket_idx % 8 Ϊ3
*/

template <typename KeyType, typename ValueType, typename KeyComparator>  // ���ÿɶ�
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {          // 0000 0000 |= 0000 1000 = 0000 1000
  readable_[bucket_idx / 8] |= 1 << (bucket_idx % 8);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {          // �����ѷ���
  occupied_[bucket_idx / 8] |= 1 << (bucket_idx % 8);                    // 0000 0000 |= 0000 1000  = 0000 1000  ����bucket_idx % 8 Ϊ3
}

//���bucket_idx����KV��
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // char ��������,ÿ����λ 8 bit,�ܱ��8��λ���Ƿ��м�ֵ�Դ���, bucket_idx / 8 
  // �ҵ�Ӧ���޸ĵ��ֽ�λ��
  // bucket_idx % 8 �ҳ��ڸ��ֽڵĶ�Ӧ bit λ pos_
  // ����һ�� 8 λ���ȵ�,���� pos_ λΪ 0 ��,����ȫΪ 1 ��Byte ,����: 11110111
  // Ȼ������Ӧ�ֽ�ȡ��λ�����,��ʵ�������λ�õ� 1 �Ĳ���,������λ�ñ��ֲ���
  readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));                // 0000 1000 &= 1111 0111 = 0000 0000  ɾ����ֵ�ԣ� ����Ϊδ��
}                                                                       //����ɾ��arry_��ļ�ֵ�ԣ��Ȳ���ʱ��Ȼ�Ḳ��

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {    //ֻҪoccupied����������ز�Ϊ 0000 0000
  return (occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;    //0000 1000 & 0000 1000 = 0000 1000  �ѷ���
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return (readable_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;    //0000 1000 & 0000 1000 = 0000 1000 �ɶ�
}




//��ӡ�洢Ͱ��ռ����Ϣ
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
