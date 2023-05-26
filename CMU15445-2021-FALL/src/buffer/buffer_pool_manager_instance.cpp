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
    // ��ҳ���б�������ݹ��㡣
    inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

    // Page�ǻ�����е�ҳ������

    The actual data that is stored within a page. 
    data_�����Ӧ����ҳ���ʵ������
    char data_[PAGE_SIZE]{};

    The ID of this page. 
    // page_id_�����ҳ���ڴ��̹������е�ҳ��ID
    page_id_t page_id_ = INVALID_PAGE_ID;

     The pin count of this page. 
    pin_count_����DBMS����ʹ�ø�ҳ����û���Ŀ
    int pin_count_ = 0;

    True if the page is dirty, i.e. it is different from its corresponding page on disk. 
    is_dirty_�����ҳ���Դ��̶����д�غ��Ƿ��޸�
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
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");  //���BPI���ǳص�һ���֣���ô�ش�СӦ��ֻ��1
  BUSTUB_ASSERT(
      instance_index < num_instances,
      //BPI�������ܴ��ڳ��е�BPI�����ڷǲ�������£�����Ӧ��Ϊ1��
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");

  // We allocate a consecutive memory space for the buffer pool.
  // ����Ϊ����ط���һ���������ڴ�ռ䡣
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  // �����ÿ��ҳ�涼�ڿ��в�λfree_list_�С�
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

// ��������
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// FlushPgImp������ʽ�ؽ������ҳ��д�ش��̡�
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) { 
    frame_id_t frame_id;
    std::lock_guard<std::mutex> lock(latch_);                       //����
    if (page_table_.count(page_id) == 0) {                          //��������в����ڶ�Ӧҳ��IDҳ��
        return false;                                               //����false
    }                                                               //������
    frame_id = page_table_[page_id];                                //��ȡ��Ӧҳ���ҳ��ID
    pages_[frame_id].is_dirty_ = false;                             //��������ڵĸ�ҳ���is_dirty_��Ϊfalse
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());  //ʹ��WritePage����ҳ���ʵ������data_д�ش���
    return true;                                                    //����true
}

//  FlushAllPgsImp��������ڵ�����ҳ��д�ش��̡�
  void BufferPoolManagerInstance::FlushAllPgsImp() { 
    std::lock_guard<std::mutex> lock(latch_);     // ����
    for (auto id : page_table_) {                 // ����page_table_�Ի�û�����ڵ�<ҳ��ID - ��λID>��
        pages_[id.second].is_dirty_ = false;      // �������������ҳ���is_dirty_��Ϊfalse
        disk_manager_->WritePage(id.first, pages_[id.second].GetData());    //ʹ��WritePage����ҳ���ʵ������data_д�ش���
    }
  }

//GetFrame()�� ��ȡframe_id�����뺯��ǰ�����
frame_id_t BufferPoolManagerInstance::GetFrame() { 
    frame_id_t new_frame_id;
    if (!free_list_.empty()) {                                  //���free_list_��Ϊ�գ����ڿ���ҳ��
        new_frame_id = free_list_.back();                       //�� frame_id����Ϊ free_list_β�ڵ��ֵ
        free_list_.pop_back();                                  //free_list_ɾ��β�ڵ�
    }
    bool res = replacer_->Victim(&new_frame_id);                //���free_list_Ϊ�գ������ڿ���ҳ��,����Victim()��frame_id��ֵ
    if (!res) {                                                 //���޿�����ҳ�棬��ֵʧ��
        return NUMLL_FRAME;                                     //����NUMLL_FRAME
    }
    page_id_t victim_page_id = pages_[new_frame_id].page_id_;   //��ȡ������ҳ��֮ǰ��ҳ��ID
    if (pages_[new_frame_id].IsDirty()) {                       //���������ҳ��Ϊ��ҳ
        disk_manager_->WritePage(victim_page_id, pages_[new_frame_id].GetData());   //д�����
    }       
    page_table_.erase(victim_page_id);                          //������ϣ��page_table_����<ҳ��ID - ����β�ڵ�>ӳ��

    return new_frame_id;                                        //������ҳ��ID
}
    
 // NewPgImp�ڴ����з����µ�����ҳ�棬�������������أ�������ָ�򻺳��ҳ��Page��ָ�롣
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) { 
    std::lock_guard<std::mutex> lock(latch_);       //����
    frame_id_t new_frame_id;
    page_id_t new_page_id;
    new_frame_id = GetFrame();                      //����GetFrame()��ȡframe_id
    if (new_frame_id == NUMLL_FRAME) {              //���û�п��в�λ�Ϳɱ������λ
        return nullptr;                             //���ؿ�ָ��
    }
    new_page_id = AllocatePage();   //����ڿɱ������λ�������`AllocatePage()`Ϊ�µ�����ҳ�����`page_id`ҳ��ID��

    page_table_[new_page_id] = new_frame_id;        //��`page_table_`��ɾ��Ŀ���λ�е�ԭҳ��ID��ӳ�䣬�����µ�<ҳ��ID - ��λID>ӳ�����

    pages_[new_frame_id].page_id_ = new_page_id;    //���²�λ��ҳ���Ԫ����
    pages_[new_frame_id].is_dirty_ = false;
    pages_[new_frame_id].pin_count_ = 1;
    pages_[new_frame_id].ResetMemory();             //��ҳ���б�������ݹ��㡣
    //��ʱ��ҳ�������ݣ��������轫���ݴӴ���д�뻺���

     /*
     ������ҳҲ��Ҫд�ش��̣���������� newpage unpin Ȼ���ٱ���̭��ȥ fetchpageʱ�ͻᱨ��
     �������в��޴�ҳ��������ֱ��is_dirty_��Ϊtrue�����Իᱨ��
     */
    disk_manager_->WritePage(new_page_id, pages_[new_frame_id].GetData());

    return &pages_[new_frame_id];                   // ����ָ�򻺳��ҳ��Page��ָ��
}

//FetchPgImp�Ĺ����ǻ�ȡ��Ӧҳ��ID��ҳ�棬������ָ���ҳ���ָ��
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id){ 
    std::lock_guard<std::mutex> lock(latch_);       //����
    frame_id_t frame_id;
    if (page_table_.count(page_id) > 0) {           //�������ش��ڶ�Ӧҳ��
        frame_id = page_table_[page_id];            //��ȡ��Ӧҳ��ID
        if (pages_[frame_id].pin_count_ == 0) {     //���֮ǰҳ��û���û�������Pin�����̶�ҳ���ֹ������
            replacer_.Pin(frame_id);                //������û������Ѿ��̶�
        }
        pages_[frame_id].pin_count_++;              //����ҳ����û�������
        return &pages_[frame_id];
    }
    frame_id = GetFrame();                          //��������û�ж�Ӧҳ�棬����GetFrame()
    if (frame_id == NUMLL_FRAME) {                  //���û�п���ҳ��Ϳ�����ҳ��
        return nullptr;                             //����nullptr
    }
                                                    
    page_table_[page_id] = frame_id;                //���¹�ϣ���ӳ��
    
    pages_[frame_id].is_dirty_ = false;             //���²�λ��ҳ���Ԫ����
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].page_id_ = page_id;

    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());   //��ҳ������д�뻺���
    return &pages_[frame_id];                       // ����ָ�򻺳��ҳ��Page��ָ��
} 

  
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);   // ����
    frame_id_t frame_id;
    if (page_table_.count(page_id) == 0) {      //����ҳ���Ƿ�����ڻ�����
        return true;                            //��δ�����򷵻�True
    }
    frame_id = page_table_[page_id];            //��ȡframe_id

    if (pages_[frame_id].pin_count_ != 0) {     //����ҳ����û���`pin_count_`�Ƿ�Ϊ0
        return false;                           //���0�򷵻�False
    }

    /*
    ����Ҫд��ҳ����ҳ��ɾ��
    if (pages_[frame_id].IsDirty) {
        pages_[frame_id].is_dirty_ = false;
        disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }
    */

    page_table_.erase(page_id);                     //ɾ����ϣ���ӳ��
    replacer_->Pin(frame_id);                       //�̶�������ҳ��ID
    free_list_.emplace_back(frame_id);              //�ڿ�������β����ӽڵ�

    pages_[frame_id].page_id_ = INVALID_PAGE_ID;    //���²�λ��ҳ���Ԫ����
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;

    DeallocatePage(page_id);                        //ɾ�������϶�Ӧ��ҳ������

    return true;
}
 
//UnpinPgImp�Ĺ���Ϊ�ṩ�û��򻺳��֪ͨҳ��ʹ����ϵĽӿڣ�
//�û�������ʹ�����ҳ���ҳ��ID�Լ�ʹ�ù������Ƿ�Ը�ҳ������޸ġ�
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);   //����
    frame_id_t frame_id;
    if (page_table_.count(page_id) == 0) {      //�����ҳ���Ƿ��ڻ������
        return true;                            //��δ�ڻ�������򷵻�True
    }
    frame_id = page_table_[page_id];            //��ȡframe_id
    Page curpage = pages_[frame_id];            //��ȡҳ��
    if (curpage.pin_count_ == 0) {              //����ҳ����û����Ƿ����0
        return false;                           //�粻�����û��򷵻�false
    }

    if (is_dirty) {                             //����û�ʹ�ù����жԸ�ҳ��������޸�
        curpage.is_dirty_ = true;               //����ҳ������Ϊ��ҳ
    }

    curpage.pin_count_--;                       //�ݼ���ҳ����û���pin_count_

    if (curpage.pin_count_ == 0) {              //���ڵݼ����ֵ����0
        replacer_->Unpin(frame_id);             //����replacer_->Unpin�Ա�ʾ��ҳ����Ա�����
    }
    return true;                                //����True
}                                   

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
                                                        //�����ҳ���޸Ļش�BPI
}

}  // namespace bustub
