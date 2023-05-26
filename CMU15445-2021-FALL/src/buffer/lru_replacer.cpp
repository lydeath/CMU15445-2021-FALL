//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*
    本部分中需要实现缓冲池中的LRUReplacer，该组件的功能是跟踪缓冲池内的页面使用情况，并在缓冲池容量不足时驱除缓冲池中最近最少使用的页面。其应当具备如下接口：

    Victim(frame_id_t*)：驱逐缓冲池中最近最少使用的页面，并将其内容存储在输入参数中。当LRUReplacer为空时返回False，否则返回True；
    Pin(frame_id_t)：当缓冲池中的页面被用户访问时，该方法被调用使得该页面从LRUReplacer中驱逐，以使得该页面固定在缓存池中；
    Unpin(frame_id_t)：当缓冲池的页面被所有用户使用完毕时，该方法被调用使得该页面被添加在LRUReplacer，使得该页面可以被缓冲池驱逐；
    Size()：返回LRUReplacer中页面的数目；
*/

#include "buffer/lru_replacer.h"
#include <iostream>
namespace bustub {

// 构造函数
LRUReplacer::LRUReplacer(size_t num_pages) {}

// 析构函数
 LRUReplacer::~LRUReplacer() = default;


 /*
 - 首先判断链表是否为空
 - 如不为空则返回链表首节点的页面ID
 - 并在哈希表中解除指向首节点的映射
 */
 bool LRUReplacer::Victim(frame_id_t *frame_id) { 

    std::lock_guard<std::mutex> lock(data_latch_);
   if (lru_list_.empty()) {
      return false;
   }
   
   *frame_id = lru_list_.back();
   lru_list_.pop_back();
   data_idx_.erase(*frame_id);
   return true;
 }

/*
* 对于`Pin`
 - 检查`LRUReplace`中是否存在对应页面ID的节点，
 - 如不存在则直接返回，
 - 如存在对应节点则通过哈希表中存储的迭代器删除链表节点，并解除哈希表对应页面ID的映射。
 */
 void LRUReplacer::Pin(frame_id_t frame_id) { 
   std::lock_guard<std::mutex> lock(data_latch_);   //上锁
   auto it = data_idx_.find(frame_id);              //寻找是否存在对应页面ID节点
   if (it == data_idx_.end()) {                     //不存在
      return;                                       //直接返回
   }                                                //如果存在
   lru_list_.erase(it->second);                     //通过哈希表存储的迭代器删除链表节点
   data_idx_.erase(it);                             //解除哈希表对应的页面ID的映射
 }

/*
* 对于`Unpin`
 - 检查`LRUReplace`中是否存在对应页面ID的节点，
 - 如存在则直接返回，
 - 如不存在则在链表尾部插入页面ID的节点，并在哈希表中插入<页面ID - 链表尾节点>映射。
 */
 void LRUReplacer::Unpin(frame_id_t frame_id) { 
     std::lock_guard<std::mutex> lock(data_latch_); //上锁
     auto it = data_idx_.find(frame_id);            //寻找是否存在对应页面ID的节点
     if (it != data_idx_.end()) {                   //如果存在（已取消固定）
        return;                                     //直接返回
     }                                              //如果不存在
     lru_list_.emplace_front(frame_id);             //在链表首部插入页面ID节点
     data_idx_[frame_id] = lru_list_.front();       //在哈希表中插入<页面ID - 链表尾节点>映射
 }

size_t LRUReplacer::Size() { 
    std::lock_guard<std::mutex> lock(data_latch_);  //上锁
    size_t ret = data_idx_.size();                  //获取哈希表大小
    return ret;                                     //返回结果
}

}  // namespace bustub

/*
* LinkListNode 双向链表
* class LinkListNode {
  public:
      frame_id_t val_{0};
      LinkListNode *prev_{nullptr};
      LinkListNode *next_{nullptr};
      explicit LinkListNode(frame_id_t Val) : val_(Val) {}
      //链表内存储的是页面id frame_id_t
};
    实际实现中使用STL中的哈希表unordered_map和双向链表list，
    并在unordered_map中存储指向链表节点的list::iterator。
*
  // TODO(student): implement me!
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> data_idx_;
  LinkListNode *head_{nullptr};
  LinkListNode *tail_{nullptr};
  std::mutex data_latch_;

  std::list<frame_id_t> lru_list_;
    */
