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
    ����������Ҫʵ�ֻ�����е�LRUReplacer��������Ĺ����Ǹ��ٻ�����ڵ�ҳ��ʹ����������ڻ������������ʱ������������������ʹ�õ�ҳ�档��Ӧ���߱����½ӿڣ�

    Victim(frame_id_t*)�����𻺳�����������ʹ�õ�ҳ�棬���������ݴ洢����������С���LRUReplacerΪ��ʱ����False�����򷵻�True��
    Pin(frame_id_t)����������е�ҳ�汻�û�����ʱ���÷���������ʹ�ø�ҳ���LRUReplacer��������ʹ�ø�ҳ��̶��ڻ�����У�
    Unpin(frame_id_t)��������ص�ҳ�汻�����û�ʹ�����ʱ���÷���������ʹ�ø�ҳ�汻�����LRUReplacer��ʹ�ø�ҳ����Ա����������
    Size()������LRUReplacer��ҳ�����Ŀ��
*/

#include "buffer/lru_replacer.h"
#include <iostream>
namespace bustub {

// ���캯��
LRUReplacer::LRUReplacer(size_t num_pages) {}

// ��������
 LRUReplacer::~LRUReplacer() = default;


 /*
 - �����ж������Ƿ�Ϊ��
 - �粻Ϊ���򷵻������׽ڵ��ҳ��ID
 - ���ڹ�ϣ���н��ָ���׽ڵ��ӳ��
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
* ����`Pin`
 - ���`LRUReplace`���Ƿ���ڶ�Ӧҳ��ID�Ľڵ㣬
 - �粻������ֱ�ӷ��أ�
 - ����ڶ�Ӧ�ڵ���ͨ����ϣ���д洢�ĵ�����ɾ������ڵ㣬�������ϣ���Ӧҳ��ID��ӳ�䡣
 */
 void LRUReplacer::Pin(frame_id_t frame_id) { 
   std::lock_guard<std::mutex> lock(data_latch_);   //����
   auto it = data_idx_.find(frame_id);              //Ѱ���Ƿ���ڶ�Ӧҳ��ID�ڵ�
   if (it == data_idx_.end()) {                     //������
      return;                                       //ֱ�ӷ���
   }                                                //�������
   lru_list_.erase(it->second);                     //ͨ����ϣ��洢�ĵ�����ɾ������ڵ�
   data_idx_.erase(it);                             //�����ϣ���Ӧ��ҳ��ID��ӳ��
 }

/*
* ����`Unpin`
 - ���`LRUReplace`���Ƿ���ڶ�Ӧҳ��ID�Ľڵ㣬
 - �������ֱ�ӷ��أ�
 - �粻������������β������ҳ��ID�Ľڵ㣬���ڹ�ϣ���в���<ҳ��ID - ����β�ڵ�>ӳ�䡣
 */
 void LRUReplacer::Unpin(frame_id_t frame_id) { 
     std::lock_guard<std::mutex> lock(data_latch_); //����
     auto it = data_idx_.find(frame_id);            //Ѱ���Ƿ���ڶ�Ӧҳ��ID�Ľڵ�
     if (it != data_idx_.end()) {                   //������ڣ���ȡ���̶���
        return;                                     //ֱ�ӷ���
     }                                              //���������
     lru_list_.emplace_front(frame_id);             //�������ײ�����ҳ��ID�ڵ�
     data_idx_[frame_id] = lru_list_.front();       //�ڹ�ϣ���в���<ҳ��ID - ����β�ڵ�>ӳ��
 }

size_t LRUReplacer::Size() { 
    std::lock_guard<std::mutex> lock(data_latch_);  //����
    size_t ret = data_idx_.size();                  //��ȡ��ϣ���С
    return ret;                                     //���ؽ��
}

}  // namespace bustub

/*
* LinkListNode ˫������
* class LinkListNode {
  public:
      frame_id_t val_{0};
      LinkListNode *prev_{nullptr};
      LinkListNode *next_{nullptr};
      explicit LinkListNode(frame_id_t Val) : val_(Val) {}
      //�����ڴ洢����ҳ��id frame_id_t
};
    ʵ��ʵ����ʹ��STL�еĹ�ϣ��unordered_map��˫������list��
    ����unordered_map�д洢ָ������ڵ��list::iterator��
*
  // TODO(student): implement me!
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> data_idx_;
  LinkListNode *head_{nullptr};
  LinkListNode *tail_{nullptr};
  std::mutex data_latch_;

  std::list<frame_id_t> lru_list_;
    */
