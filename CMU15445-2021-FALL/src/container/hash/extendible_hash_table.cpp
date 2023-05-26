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
    由缓冲池管理器支持的可扩展哈希表的实现。
    支持非唯一密钥。支持插入和删除。
    当存储桶变满/变空时，表会动态增长/收缩。

  //在构造函数中，为哈希表分配一个目录页面和桶页面，并设置目录页面的page_id成员、
  //将哈希表的首个目录项指向该桶。最后，不要忘记调用UnpinPage向缓冲池告知页面的使用完毕。
 */
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
// Hash-将MurmurHash的64位哈希向下转换为32位的简单助手
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

// 获得哈希键在目录页面的桶页面索引
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hashed_key = Hash(key);
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return mask & hashed_key;
}

// 获取哈希键，通过从目录页上获得的桶页面索引获得对应的桶页面的桶ID
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(idx);
}

// 从缓冲池管理器获取目录页。
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

// 使用存储桶的page_id从缓冲池管理器中获取存储桶页面。
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

///////////////////////////////////////
//  每次Fetch时，页面如果存在都会被设置为Pin，要注意用完要UnPinr


/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
    GetValue从哈希表中读取与键匹配的所有值结果，其通过哈希表的读锁保护目录页面，
    并使用桶的读锁保护桶页面。具体的操作步骤为
    1.先读取目录页面，
    2.再通过目录页面和哈希键或许对应的桶页面，
    3.最后调用桶页面的GetValue获取值结果。
    在函数返回时注意要UnpinPage所获取的页面。加锁时应当保证锁的获取、释放全局顺序以避免死锁
*/
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

/*****************************************************************************
 * INSERTION
 * Insert向哈希表插入键值对，这可能会导致桶的分裂和表的扩张，因此需要保证目录页面的读线程安全，
 一种比较简单的保证线程安全的方法为：在操作目录页面前对目录页面加读锁。但这种加锁方式使得Insert
 函数阻塞了整个哈希表，这严重影响了哈希表的并发性。可以注意到，表的扩张的发生频率并不高，对目录
 页面的操作属于读多写少的情况，因此可以使用乐观锁的方法优化并发性能，其在Insert被调用时仅保持读
 锁，只在需要桶分裂时重新获得读锁。

Insert函数的具体流程为：

获取目录页面和桶页面，在加全局读锁和桶写锁后检查桶是否已满，如已满则释放锁，并调用UnpinPage释放页
面，然后调用SplitInsert实现桶分裂和插入；
如当前桶未满，则直接向该桶页面插入键值对，并释放锁和页面即可。
 *****************************************************************************/
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



/*
*使用可选的桶拆分执行插入。如果页面在分割后仍然是满的，然后递归分割。
*这种情况极为罕见，但有可能。
* 
* 由于SplitInsert比较复杂，这里进行分段讲解：
    首先，获取目录页面并加全局写锁，在添加全局写锁后，其他所有线程均被阻塞了，
    因此可以放心的操作数据成员。
    不难注意到，在Insert中释放读锁和SplitInsert中释放写锁间存在空隙，其他线程可能在该空隙中被调度，从而改变桶页面或目录页面数据。
    因此，在这里需要重新在目录页面中获取哈希键所对应的桶页面（可能与Insert中判断已满的页面不是同一页面），并检查对应的桶页面是否已满。
    如桶页面仍然是满的，则分配新桶和提取原桶页面的元数据。
    在由于桶分裂后仍所需插入的桶仍可能是满的，因此在这这里进行循环以解决该问题。
*/
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



/*****************************************************************************
 * REMOVE
 *      Remove从哈希表中删除对应的键值对，其优化思想与Insert相同，由于桶的合并并不频
     繁，因此在删除键值对时仅获取全局读锁，只在需要合并桶时获取全局写锁。当删除后桶
     为空且目录项的局部深度不为零时，释放读锁并调用Merge尝试合并页面，随后释放锁和页面并返回。

 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
    
}



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
}

/*****************************************************************************
 * MERGE
 * 在Merge函数获取写锁后，需要重新判断是否满足合并条件，以防止在释放锁的空隙时页面被更改，
 * 在合并被执行时，需要判断当前目录页面是否可以收缩，如可以搜索在这里仅需递减全局深度即可完
 * 成收缩，最后释放页面和写锁。
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();                                          //获得最新目录页面     
  table_latch_.WLock();                                                                             //上写锁
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
