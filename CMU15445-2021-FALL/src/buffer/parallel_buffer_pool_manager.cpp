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

// ���ѿ��������������ʵ�ֵ����������������ȹ������ڽ����κ�һ�����ʱ���������������ס����˼��������ڲ����ԡ������
// ���л���ص�˼���Ƿ����������Ļ���أ�������ͬ��ҳ��IDӳ�������ԵĻ�����У��Ӷ��������建��ص������ȣ������Ӳ����ԡ�
namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  num_instances_ = num_instances;
  pool_size_ = pool_size;
  start_idx_ = 0;
  // resize�Ժ��ڲ�ȫ��ʼ��Ϊ0��push_back���Ǵ�����0��ʼ������ֱ����ӵ�����num_instances + 1
  //instances_.resize(num_instances);  
  for (size_t i = 0; i < num_instances; i++) {
      //����ָ��
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

//GetPoolSizeӦ����ȫ������ص�����������������ظ������Ի����������
size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances  ��ȡ����BufferPoolManagerʵ���Ĵ�С
  return num_instances_ * pool_size_;  // ����ظ��� * ���������
}

//GetBufferPoolManager����ҳ��ID����Ӧ�Ķ��������ָ�룬�����ͨ����ҳ��IDȡ��ķ�ʽ��ҳ��IDӳ������Ӧ�Ļ���ء�
BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  // ��BufferPoolManager�����������ҳ��id��������������������ʹ�ô˷�����
  return instances_[page_id % num_instances_];
}


//************************************************************************************************************
Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  //�Ӹ����BufferPoolManagerInstance��ȡpage_id��ҳ��
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  //�Ӹ����BufferPoolManagerInstance��ȡ���̶�page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  // �Ӹ����BufferPoolManagerInstanceˢ��page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->FlushPage(page_id);
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  // �Ӹ����BufferPoolManagerInstance��ɾ��page_id
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  // ˢ������BufferPoolManagerʵ���е�����ҳ��
  for (size_t i = 0; i < num_instances_; i++) {
    instances_[i]->FlushAllPages();
  }
}

// ��������������ö�Ӧ��������صķ������ɡ�ֵ��ע����ǣ������ڻ�����д�ŵ�Ϊ�����ʵ����Ļ���ָ�룬���������
// ������ӦΪ�����ʵ����Ļ����Ӧ���麯�������ң�����ParallelBufferPoolManager��BufferPoolManagerInstanceΪ�ֵܹ�ϵ
// �����ParallelBufferPoolManager����ֱ�ӵ���BufferPoolManagerInstance��Ӧ��Imp���������ֱ����ParallelBufferPoolMan
// ager�д��BufferPoolManagerInstanceָ��Ҳ�ǲ����е�(����ParallelBufferPoolManager��ֻ�ܴ��BufferPoolManager��ָ��)
// 
//                          ParallelBufferPoolManager::BufferPoolManager
//                          BufferPoolManagerInstance::BufferPoolManager
// 
// 
//***************************************************************************************************************************

//�����Ϊ��ʹ�ø���������صĸ��ؾ��⣬������ת����ѡȡ��������ҳ��ʱʹ�õĻ���أ����������Ĺ������£�
//��start_idx_��ʼ��������������أ�����ڵ���NewPage�ɹ���ҳ�棬�򷵻ظ�ҳ�沢��start_idxָ���ҳ�����һ��ҳ�棻
//��ȫ������ص���NewPage��ʧ�ܣ��򷵻ؿ�ָ�룬������start_idx�� 
// 

  // ������ҳ�档���ǽ���ѭ����ʽ�ӵײ�����ҳ�����
  // BufferPoolManagerʵ��
  //  1.   ��BPMI����ʼ�����У�����NewPageImpl��ֱ��
  //        1���ɹ���Ȼ�󷵻�
  //        2��ѭ������ʼ����������nullptr
  //  2.   ÿ�ε��ô˺���ʱ��������ʼ������modʵ���������ڲ�ͬ��BPMI����ʼ����
Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
    Page *ret;
    for (size_t i = 0; i < num_instances_; i++) {
        size_t idx = (start_idx_ + i) % num_instances_;
        if ((ret = instances_[idx]->NewPage()(page_id)) != nullptr) {   //���������ҳ��ɹ�
            start_idx_ = (*page_id + 1) % num_instances_;               //��һ�ο�ʼ������Ϊ��ҳ�����һ������
            return ret;                                                 //���ش�������ҳ��
        }
    }
    start_idx_++;   //�������������Ȼû�д����ɹ�����ʼ����+1��������ԭʼλ��
    return nullptr; //����nullptr
}


}  // namespace bustub
