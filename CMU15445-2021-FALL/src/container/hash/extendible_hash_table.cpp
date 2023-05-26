//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
namespace bustub {

/*
    �ɻ���ع�����֧�ֵĿ���չ��ϣ���ʵ�֡�
    ֧�ַ�Ψһ��Կ��֧�ֲ����ɾ����
    ���洢Ͱ����/���ʱ����ᶯ̬����/������

  //�ڹ��캯���У�Ϊ��ϣ�����һ��Ŀ¼ҳ���Ͱҳ�棬������Ŀ¼ҳ���page_id��Ա��
  //����ϣ����׸�Ŀ¼��ָ���Ͱ����󣬲�Ҫ���ǵ���UnpinPage�򻺳�ظ�֪ҳ���ʹ����ϡ�
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // LOG_DEBUG("BUCKET_ARRAY_SIZE = %ld", BUCKET_ARRAY_SIZE);

    //����Ŀ¼ҳ�棬ǿ��ת��������ȫ����data_��,��Ӱ������Ԫ����
    HashTableDirectoryPage *dir_page =              //����Ŀ¼ҳ��ΪĿ¼ҳ����page_id
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
    dir_page->SetPageId(directory_page_id_);        //����Ŀ¼ҳ��ID

    //����Ͱҳ��
    page_id_t new_bucket_id;
    buffer_pool_manager->NewPage(&new_bucket_id);   // �����һ��Ͱ��ҳ

    dir_page->SetBucketPageId(0, new_bucket_id);    //����Ŀ¼ҳ��bucket_page_ids_(bucket_idx, bucket_page_id)

    //����UnpinPage�򻺳�ظ�֪ҳ���ʹ�����
  //auto UnpinPage(page_id_t page_id, bool is_dirty, bufferpool_callback_fn callback = nullptr) -> bool
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));  //ȡ���̶�������Ϊ��ҳ
  assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
// Hash-��MurmurHash��64λ��ϣ����ת��Ϊ32λ�ļ�����
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

// ��ù�ϣ����Ŀ¼ҳ���Ͱҳ������
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hashed_key = Hash(key);
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return mask & hashed_key;
}

// ��ȡ��ϣ����ͨ����Ŀ¼ҳ�ϻ�õ�Ͱҳ��������ö�Ӧ��Ͱҳ���ͰID
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(idx);
}

// �ӻ���ع�������ȡĿ¼ҳ��
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

// ʹ�ô洢Ͱ��page_id�ӻ���ع������л�ȡ�洢Ͱҳ�档
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

///////////////////////////////////////
//  ÿ��Fetchʱ��ҳ��������ڶ��ᱻ����ΪPin��Ҫע������ҪUnPinr


/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
    GetValue�ӹ�ϣ���ж�ȡ���ƥ�������ֵ�������ͨ����ϣ��Ķ�������Ŀ¼ҳ�棬
    ��ʹ��Ͱ�Ķ�������Ͱҳ�档����Ĳ�������Ϊ
    1.�ȶ�ȡĿ¼ҳ�棬
    2.��ͨ��Ŀ¼ҳ��͹�ϣ�������Ӧ��Ͱҳ�棬
    3.������Ͱҳ���GetValue��ȡֵ�����
    �ں�������ʱע��ҪUnpinPage����ȡ��ҳ�档����ʱӦ����֤���Ļ�ȡ���ͷ�ȫ��˳���Ա�������
*/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {  
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();                      //���Ŀ¼ҳ
  table_latch_.RLock();                                                         //�ϱ��������ΪĿ¼ҳ��
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);                        //���ͰID
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);             //���Ͱҳ��
  Page *p = reinterpret_cast<Page *>(bucket);                                   //��Ͱҳ��ǿתΪPage
  p->RLatch();                                                                  //Ͱҳ���϶���(ҳ��)
  bool ret = bucket->GetValue(key, comparator_, result);                        //��ȡ��Ӧֵ������result�У�����True
  p->RUnlatch();                                                                //Ͱҳ������(ҳ����)
  table_latch_.RUnlock();                                                       //������
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));  //ȡ���̶�
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));

  return ret;
}

/*****************************************************************************
 * INSERTION
 * Insert���ϣ������ֵ�ԣ�����ܻᵼ��Ͱ�ķ��Ѻͱ�����ţ������Ҫ��֤Ŀ¼ҳ��Ķ��̰߳�ȫ��
 һ�ֱȽϼ򵥵ı�֤�̰߳�ȫ�ķ���Ϊ���ڲ���Ŀ¼ҳ��ǰ��Ŀ¼ҳ��Ӷ����������ּ�����ʽʹ��Insert
 ����������������ϣ��������Ӱ���˹�ϣ��Ĳ����ԡ�����ע�⵽��������ŵķ���Ƶ�ʲ����ߣ���Ŀ¼
 ҳ��Ĳ������ڶ���д�ٵ��������˿���ʹ���ֹ����ķ����Ż��������ܣ�����Insert������ʱ�����ֶ�
 ����ֻ����ҪͰ����ʱ���»�ö�����

Insert�����ľ�������Ϊ��

��ȡĿ¼ҳ���Ͱҳ�棬�ڼ�ȫ�ֶ�����Ͱд������Ͱ�Ƿ����������������ͷ�����������UnpinPage�ͷ�ҳ
�棬Ȼ�����SplitInsertʵ��Ͱ���ѺͲ��룻
�統ǰͰδ������ֱ�����Ͱҳ������ֵ�ԣ����ͷ�����ҳ�漴�ɡ�
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    HashTableDirectoryPage *dir_page = FetchDirectoryPage();                    //��ȡĿ¼ҳ��
    table_latch_.RLock();                                                       //Ŀ¼ҳ���϶���
    page_id_t bucket_page_id = KeyToPageId(key, dir_page);                      //ͨ����Ŀ¼ҳ�ϻ�õ�Ͱҳ��������ö�Ӧ��Ͱҳ���ͰID
    HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);           //��ȡͰҳ��
    Page *p = reinterpret_cast<Page *>(bucket);                                 //Ϊ��Ͱҳ�������ǿ��ת��
    p->WLatch();                                                                //Ͱҳ�����д��
    if (bucket->IsFull) {                                                       //���Ͱ��������Ҫ����
        p->WUnlatch();                                                          //Ͱҳ��д��
        table_latch_.RUnlock();                                                 //Ŀ¼ҳ�����
        assert(buffer_pool_manager_->UnpinPage(bucket, true, nullptr));         //Ͱҳ��ȡ���̶�������Ϊ��ҳ
        assert(buffer_pool_manager_->UnpinPage(dir_page, true, nullptr));       //Ŀ¼ҳ��ȡ���̶�������Ϊ��ҳ
        return SplitInsert(transaction, key, value);                            //���÷��Ѳ��뺯��
    }
    bool ret = bucket->Insert(key, value, comparator_);                         //���Ͱ������ѣ�ֱ�Ӳ����ֵ��
    p->WUnlatch();                                                              //Ͱҳ����д��
    table_latch_.RUnlock();                                                     //����
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr)); //Ŀ¼ҳ��ȡ���̶�
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));     //Ͱҳ��ȡ���̶�
    return ret;
}



/*
*ʹ�ÿ�ѡ��Ͱ���ִ�в��롣���ҳ���ڷָ����Ȼ�����ģ�Ȼ��ݹ�ָ
*���������Ϊ���������п��ܡ�
* 
* ����SplitInsert�Ƚϸ��ӣ�������зֶν��⣺
    ���ȣ���ȡĿ¼ҳ�沢��ȫ��д���������ȫ��д�������������߳̾��������ˣ�
    ��˿��Է��ĵĲ������ݳ�Ա��
    ����ע�⵽����Insert���ͷŶ�����SplitInsert���ͷ�д������ڿ�϶�������߳̿����ڸÿ�϶�б����ȣ��Ӷ��ı�Ͱҳ���Ŀ¼ҳ�����ݡ�
    ��ˣ���������Ҫ������Ŀ¼ҳ���л�ȡ��ϣ������Ӧ��Ͱҳ�棨������Insert���ж�������ҳ�治��ͬһҳ�棩��������Ӧ��Ͱҳ���Ƿ�������
    ��Ͱҳ����Ȼ�����ģ��������Ͱ����ȡԭͰҳ���Ԫ���ݡ�
    ������Ͱ���Ѻ�����������Ͱ�Կ��������ģ���������������ѭ���Խ�������⡣
*/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    HashTableDirectoryPage *dir_page = FetchBucketPage();                                                   // ���»�ȡĿ¼ҳ��
    table_latch_.WLock();
    while (true) {
        uint32_t bucket_idx = KeyToDirectoryIndex(ket, dir_page);                                           //��ȡĿ¼����
        page_id_t bucket_page_id = KeyToPageId(key, dir_page);                                              //ͨ��Ŀ¼������ȡͰҳ��ID
        HASH_TABLE_BUCKET_TYPE *bucket = FetchDirectoryPage(bucket_page_id);                                //��ȡͰҳ��ID

        if (bucket->IsFull) {                                                                               //���Ͱ�������ģ�����
            uint32_t global_depth = dir_page->GetGlobalDepth();                                             //��ȡȫ�����
            uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);                                     //��ȡҪ����Ͱ�ľֲ����
            page_id_t new_bucket_id = 0;
            HASH_TABLE_BUCKET_TYPE *new_bucket =                                                            //��ȡһ��������ҳ������ŷ��Ѻ����Ͱ
                reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_id));
            assert(new_bucket != nullptr);

            if (global_depth == local_depth) {                                                              //����ֲ���ȵ���ȫ����ȣ�����Ŀ¼
                uint32_t bucket_num = 1 << global_depth;                                                    //��ȡͰ������
                for (uint32_t i = 0; i < bucket_num; i++) {                                                 
                    dir_page->SetBucketPageId(i + bucket_num, dir_page->GetBucketPageId(i));                //���÷���Ŀ¼ҳ��
                    dir_page->SetLocalDepth(i + bucket_num, dir_page->GetLocalDepth(i));                    //���÷���Ŀ¼��Ͱ�ľֲ����
                }
                dir_page->IncrGlobalDepth();                                                                //���Ѻ�ȫ����ȼ�һ
                dir_page->SetBucketPageId(bucket_idx + bucket_num, new_bucket_id);                          //���¹�ϣ��ָ��ķ���Ͱ
                dir_page->IncrLocalDepth(bucket_idx);                                                       //���ԭͰ�ֲ����
                dir_page->IncrLocalDepth(bucket_idx + bucket_num);                                          //��ӷ���Ͱ�ֲ����
                global_depth++;                                                                             //ȫ����ȼ�һ
            } else {                //����ֲ����С��ȫ����ȣ������Ͱ���������Ŀ¼
                /*  i = GD, j = LD
                   �ֵ�Ŀ¼���е���ˣ�λ��ʾ��С��Ŀ¼��Ϊ��jλ���䡢����λΪ0��Ŀ¼�
                   ��������Ŀ¼��Ĺ�ϣ�����    step = 1<<j
                   ���Ѻ����������ֵ�Ŀ¼��Ĺ�ϣ����� step*2
                   �ֵ�Ŀ¼�������Ϊ1<<(i - j)��
                   ��Ҫ���ĵ���Ϊ 1<<(i - j - 1)
                */
                // �˴�Ϊold_mask��Ϊ111  new_maskΪ1111
                uint32_t mask = (1 << local_depth) - 1;     // 2^1 - 1 = 1 -> 0001

��              // ��ʼID
                // 0111 & 1111 = 0111����Ϊ����ǰͰID����base_idx
                uint32_t base_idx = mask & bucket_idx;

                // ��Ҫ���ĵ�����������GD=2, LD=1 recordes_num = 1; GD=3, LD=2 recordes_num = 1; GD=3, LD=1 recordes_num = 2
                uint32_t records_num = 1 << (global_depth - local_depth - 1);  // 2 ^ (2 - 1 - 1)

                // ������� LD = 1��step = 2;  LD = 2��step = 4
                uint32_t step = (1 << local_depth);  // 2^local_depth
                uint32_t idx = base_idx;

                // ���ȱ���һ��Ŀ¼������ָ���Ͱ��λ����ȼ�һ
                for (uint32_t i = 0; i < records_num; i++) {
                    dir_page->IncrLocalDepth(idx);  // Ŀ¼��ӦԭͰ�ֲ���ȼ�һ
                    idx += step * 2;
                }

                // ���������Ƿ�Ӱ��ȫ����ȣ��Ը�λ�ý��в���
                idx = base_idx + step;
                for (uint32_t i = 0; i < records_num; i++) {  // Ŀ¼���ֲ���ȼ�һ������Ŀ¼ָ����Ͱ��������ȼ�һ
                    dir_page->SetBucketPageId(idx, new_bucket_id);
                    dir_page->IncrLocalDepth(idx);
                    idx += step * 2;
                }
            }
            /*
                �����Ͱ���Ѻ�Ӧ����ԭͰҳ���еļ�¼���²����ϣ�����ڼ�¼�ĵ�i-1λ����ԭͰҳ�����Ͱҳ���Ӧ��
                ��˼�¼�����Ͱҳ�������ΪԭͰҳ�����Ͱҳ������ѡ�������²������¼���ͷ���Ͱҳ���ԭͰҳ�档
            */
            for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
                KeyType j_key = bucket->KeyAt(i);                                   //��ȡͰ�ڲ۵Ĺ�ϣ��
                ValueType j_value = bucket->ValueAt(i);                             //��ȡ�۵�ֵ
                bucket->RemoveAt(i);                                                //ɾ��Ͱ�ڲ۵ļ�ֵ��
                if (KeyToPageId(j_key, dir_page) == bucket_page_id) {               //�����ϣ������ԭͰ
                    bucket->Insert(j_key, j_value, comparator_);                    //����ԭͰ��
                } else {                                                            //�����ϣ�����ڷ���Ͱ
                    new_bucket->Insert(j_key, j_value, comparator_);                //�������Ͱ��
                }
            }
            assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));  // ԭͰҳ��ȡ��ȷ��
            assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));   // ��Ͱҳ��ȡ���̶�
        } else {
            bool ret = bucket->Insert(key, value, comparator_);                         // ֱ�Ӳ���
            table_latch_.WUnlock();                                                     // Ŀ¼ҳ��д��
            assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr)); // Ŀ¼ҳȡ���̶�
            assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));     // Ͱҳȡ���̶�
            return ret;
        }
    }
    return false;
}



/*****************************************************************************
 * REMOVE
 *      Remove�ӹ�ϣ����ɾ����Ӧ�ļ�ֵ�ԣ����Ż�˼����Insert��ͬ������Ͱ�ĺϲ�����Ƶ
     ���������ɾ����ֵ��ʱ����ȡȫ�ֶ�����ֻ����Ҫ�ϲ�Ͱʱ��ȡȫ��д������ɾ����Ͱ
     Ϊ����Ŀ¼��ľֲ���Ȳ�Ϊ��ʱ���ͷŶ���������Merge���Ժϲ�ҳ�棬����ͷ�����ҳ�沢���ء�

 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
    
}



template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();                      //���Ŀ¼ҳ
  table_latch_.RLock();                                                         //�϶���
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);                     //���Ŀ¼ҳ���Ͱҳ����
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);                        //���Ŀ¼ҳ���ͰҳID
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);             //���Ͱҳ��
  Page *p = reinterpret_cast<Page *>(bucket);
  p->WLatch();
  bool ret = bucket->Remove(key, value, comparator_);                           //ɾ����Ӧ��ֵ��
  p->WUnlatch();
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {          //���Ͱɾ����Ϊ���Ҿֲ���Ȳ�Ϊ0
    table_latch_.RUnlock();                                                     //��������
    this->Merge(transaction, key, value);                                       //�ϲ�
  } else {                                                                      //���Ͱ��Ϊ�գ�ֱ�ӽ������
    table_latch_.RUnlock();                                         
  }
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));       //Ͱҳ��ȡ���̶�,����Ϊ��ҳ
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));   //Ŀ¼ҳ��ȡ���̶�,����Ϊ��ҳ
  return ret;
}

/*****************************************************************************
 * MERGE
 * ��Merge������ȡд������Ҫ�����ж��Ƿ�����ϲ��������Է�ֹ���ͷ����Ŀ�϶ʱҳ�汻���ģ�
 * �ںϲ���ִ��ʱ����Ҫ�жϵ�ǰĿ¼ҳ���Ƿ����������������������������ݼ�ȫ����ȼ�����
 * ������������ͷ�ҳ���д����
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();                                          //�������Ŀ¼ҳ��     
  table_latch_.WLock();                                                                             //��д��
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);                                 //��Ŀ¼ҳ���ȡͰ����
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);                                 //������±�ɾ����ֵ�Ե�Ͱҳ��
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {                              //��ͰΪ���Ҿֲ���Ȳ�Ϊ0
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);                                     //��ȡ�ֲ����
    uint32_t global_depth = dir_page->GetGlobalDepth();                                             //��ȡȫ�����
    // ����ҵ�Ҫ�ϲ���bucket��
    // �𣺺ϲ���ָ��Merged Bucket�ļ�¼��
    // ������ͬ�ĵͣ�local_depth-1��λ
    // ��ˣ���ת��local_depth���Ի��Ҫ�ϲ���bucket��idx��
    uint32_t merged_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));                             //��ȡĿ¼����ֵ�Ͱ����
    page_id_t merged_page_id = dir_page->GetBucketPageId(merged_bucket_idx);                        //��ȡҪ�ϲ���bucket��ҳ��ID
    HASH_TABLE_BUCKET_TYPE *merged_bucket = FetchBucketPage(merged_page_id);                        //��ȡ���µ�Ҫ�ϲ���bucketҳ��
    if (dir_page->GetLocalDepth(merged_bucket_idx) == local_depth && merged_bucket->IsEmpty()) {    //����ϲ���Bucketҳ��ֲ���ȵ���ɾ��Bucketҳ��ľֲ���ȣ���Ϊ��Ͱ
      local_depth--;     //�ֲ����-1��Ϊ�������ǰ����������Ӱ��Ŀ¼�б���ľֲ����
      // �˴�Ϊ��ȥ�����룬����111��ԭ����Ϊ1111
      uint32_t mask = (1 << local_depth) - 1;  
      // 0111 & 1111 = 0111
      uint32_t idx = mask & bucket_idx;
      // �ֵ�Ŀ¼�������Ϊ1 << (i - j)��
      uint32_t records_num = 1 << (global_depth - local_depth);
      // ��������Ŀ¼��Ĺ�ϣ����� step = 1 << j 
      // ���Ѻ����������ֵ�Ŀ¼��Ĺ�ϣ����� step * 2
      uint32_t step = (1 << local_depth);

      for (uint32_t i = 0; i < records_num; i++) {                  //����
        dir_page->SetBucketPageId(idx, bucket_page_id);             //��Ŀ¼���������ҳ���ҳ��ID����Ϊһ����
        dir_page->DecrLocalDepth(idx);
        idx += step;
      }
      buffer_pool_manager_->DeletePage(merged_page_id);             //ɾ�����ϲ���ҳ��
    }
    if (dir_page->CanShrink()) {        //�ж�Ŀ¼ҳ���Ƿ�����
      dir_page->DecrGlobalDepth();
    }
    assert(buffer_pool_manager_->UnpinPage(merged_page_id, true, nullptr));     //�ϲ�ҳ��ȡ��ȷ��
  }
  table_latch_.WUnlock();                                                       //�����
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));   //Ŀ¼ҳ��ȡ���̶�
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));       //ɾ��ҳ��ȡ���̶�
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
