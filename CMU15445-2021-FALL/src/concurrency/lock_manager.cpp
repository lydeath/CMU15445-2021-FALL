//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <iostream>
#include <utility>
#include <vector>
namespace bustub {


//����
//��LockShared�У�����txn����Ԫ��IDΪrid�Ķ���
auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {

  // ����ǰ���жϣ��������״̬ΪABORTʱ��ֱ�ӷ��ؼ٣�
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // �統ǰ������״̬Ϊ������ʱ�����û�ȡ����������������ABORT���׳��쳣��
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // ������ĸ��뼶���READ_UNCOMMITTEDʱ���䲻Ӧ��ȡ���������Ի�ȡ����������ABORT���׳��쳣��
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  /*
      ��ǰ���ж�ͨ�����򽫵�ǰ����״̬��ΪGROWING������û����������������������ݽṹ��
      Ȼ�󣬻�ȡ��ӦԪ��ID����������м�����س�Ա��������ǰ����������������С�����
      ��txn_table_Ϊ����<����ID������>�Ķ�Ԫ�顣��Ҫע�⣬�ڸ����󱻼������ʱ����Ӧ��
      ����GetSharedLockSet()����Ԫ��ID��������ĳ��������ϣ�ʹ�ø�����ɱ��ʱ�ܽ�������
      �Ӷ�����ɾ����

      Ϊ�˱���������������Ҫ��鵱ǰ�����Ƿ����ʹ������������������������жϵ�ǰ��
      ������ȼ��Ƿ���ڸ����������������ɱ�������������can_grant��Ϊfalse��ʾ
      ���񽫱��������������ɱ�����κ�����������ͨ����������е���������cv����������
      ����������ʹ�ñ�ɱ������������˳�������С�
  */
  txn->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];              //ͨ��rid������������
  auto &request_queue = lock_request_queue.request_queue_;  //����������
  auto &cv = lock_request_queue.cv_;                        //�������֪ͨ��rid�ϱ���ֹ������
  auto txn_id = txn->GetTransactionId();                    //�������ID
  request_queue.emplace_back(txn_id, LockMode::SHARED);     //����ǰ�����������������
  txn->GetSharedLockSet()->emplace(rid);                    //��������������еĹ�������Ԫ�鼯
  txn_table_[txn_id] = txn;                                 //���������
  //Wound Wait : Kill all low priority transaction
  bool can_grant = true;
  bool is_kill = false;
  for (auto &request : request_queue) {                     //��ѯ�������
    if (request.lock_mode_ == LockMode::EXCLUSIVE) {        //����ǰԪ����Ϊд�����������ظ��ӣ�����ɱ��
      if (request.txn_id_ > txn_id) {                       //��ǰ��������ȼ����ڸ���������ȼ���ɱ������
        txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      } else {                                              //���������������
        can_grant = false;
      }
    }
    if (request.txn_id_ == txn_id) {        //����������������
      request.granted_ = can_grant;         //�����Ƿ�����
      break;
    }
  }

  //�������ɱ�����κ�����������ͨ����������е���������cv���������ȴ���������
  //ʹ�ñ�ɱ������������˳�������С�
  if (is_kill) {    
    cv.notify_all();
  }
  //Wait the lock
  while (!can_grant) {      //�������������������������Ҹ������ܽ���ɱ���������ѭ���ȴ�����
    for (auto &request : request_queue) {

      //�������ڸ�����֮ǰ���������Ƿ���ڻ�д����״̬��ΪABORT�������������������
      if (request.lock_mode_ == LockMode::EXCLUSIVE &&      
          txn_table_[request.txn_id_]->GetState() != TransactionState::ABORTED) {
        break;
      }

      //���ܱ��������˵��ǰ�������������򽫸������granted_��Ϊ�棬�����ء�
      if (request.txn_id_ == txn_id) {  
        can_grant = true;
        request.granted_ = true;
      }
    }

    //��������껹�Ǵ��������������������������cv��wait����������ԭ�ӵ��ͷ�����
    if (!can_grant) {
      cv.wait(lk);
    }

    //���������Ƿ�ɱ�����������׳��쳣
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return true;
}


//д��
//LockExclusiveʹ������txn���Ի��Ԫ��IDΪrid��Ԫ��д����
auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
//����ǰ�ü�飬�統ǰ����״̬ΪABORT���ؼ�
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
// �統ǰ���������׶Σ�����״̬��ΪABORT���׳��쳣
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }


  /*
  ��������״̬����ȡ��������У����������������У��Լ����������������ӵ�������ϡ�
  
  ��ѯ�Ƿ��н����������������������󣬵���ȡд��ʱ�������е��κ�һ�������󶼽������
  ����������������������ȼ���ʱ������ɱ��������ڲ���ɱ��������������񽫱�������
  ��ɱ������һ����ʱ�������Ѹ����ȴ����е���������
  */
  txn->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  request_queue.emplace_back(txn_id, LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn_table_[txn_id] = txn;
  //Wound Wait
  bool can_grant = true;
  bool is_kill = false;
  for (auto &request : request_queue) {
    if (request.txn_id_ == txn_id) {
      request.granted_ = can_grant;
      break;
    }
    if (request.txn_id_ > txn_id) {
      txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
      is_kill = true;
    } else {
      can_grant = false;
    }
  }
  if (is_kill) {
    cv.notify_all();
  }
  //Wait lock
  /*
  �ȴ������ã�ÿ�����񱻻���ʱ��������Ƿ�ɱ�����类ɱ�����׳��쳣����δ��ɱ����
  �������ǰ�Ƿ�������δ��ɱ������������û������������������granted_��Ϊ�档
  */
  while (!can_grant) {
    auto it = request_queue.begin();
    while (txn_table_[it->txn_id_]->GetState() == TransactionState::ABORTED) {
      ++it;
    }
    if (it->txn_id_ == txn_id) {
      can_grant = true;
      it->granted_ = true;
    }
    if (!can_grant) {
      cv.wait(lk);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return true;
}

//LockUpgrade���ڽ���ǰ����txn��ӵ�е�Ԫ��IDΪrid�Ķ�������Ϊд����
auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  /*
  �жϵ�ǰ�����Ƿ�ɱ�����Լ���Ԫ��������������Ƿ��Ѿ����ڵȴ�����������������
  ������ɱ�������׳��쳣����ͨ�����飬�򽫵�ǰ��������е�upgrading_��Ϊ��ǰ����
  ID������ʾ�ö��д���һ���ȴ�������������
  */
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];      //��ȡ��Ԫ���������б�
  if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {    //���������txn_id������У�
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  // ����ǰ��������е�upgrading_��Ϊ��ǰ����ID������ʾ�ö��д���һ���ȴ�������������
  lock_request_queue.upgrading_ = txn_id;   
  bool can_grant = false;
  /*
  ��Wound Wait�в�δ�ἰ�йظ���������Ϊ�������ｫ��ÿ�λ��ѳ�����������Ϊһ��д����ȡ��
  ��ÿ���䳢��������ʱ����ɱ������ǰ����������������

  ����巽��Ϊ��ÿ�����񱻻���ʱ���ȼ�����Ƿ�ɱ����Ȼ������������������ǰ��������
  �������ȼ��ϵ�����ɱ�����������ȼ��ϸ���can_grant��Ϊ�٣�ʾ���佫��֮��������
  ��ɱ������һ��������������������can_grantΪ��������������Ϊ��������������
  lock_mode_����upgrading_��ʼ����
  
  �������ɹ�ʱ�����������ӵ�������ϡ�
  */
  while (!can_grant) {
    auto it = request_queue.begin();
    auto target = it;
    can_grant = true;
    bool is_kill = false;
    while (it != request_queue.end() && it->granted_) {
      if (it->txn_id_ == txn_id) {              //Ϊ������������
        target = it;
      } else if (it->txn_id_ > txn_id) {        //�������ȼ��ϵ�����ɱ��
        txn_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      } else {                      
        can_grant = false;         //�������ȼ��ϸ���can_grant��Ϊ�٣�ʾ���佫��֮��������
      }
      ++it;
    }
    if (is_kill) {
      cv.notify_all();
    }
    if (!can_grant) {   //�������
      cv.wait(lk);      //����
    } else {
      target->lock_mode_ = LockMode::EXCLUSIVE;         //��������Ϊд��
      lock_request_queue.upgrading_ = INVALID_TXN_ID;   //���ø�����ϣ�ȡ����������־
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  //�������ɹ�ʱ�����������ӵ�������ϡ�
  txn->GetSharedLockSet()->erase(rid);      //����������¼
  txn->GetExclusiveLockSet()->emplace(rid); //���д����¼
  return true;
}


//����
//Unlock����ʹ������txn�ͷ�Ԫ��IDΪridԪ���ϵ�����
/*
��Ҫע�⣬��������뼶��ΪREAD_COMMITʱ�������õĶ�������ʹ����Ϻ������ͷţ�
��˸������񲻷���2PL����Ϊ�˳���ļ����ԣ���������ΪREAD_COMMIT������COMMIT��ABORT
֮ǰʼ�ձ���GROWING״̬�������������񣬽��ڵ���Unlockʱת��ΪSHRINKING״̬�����ͷ���ʱ
��������������в�ɾ����Ӧ�����������Ȼ�����������񣬲����������������ɾ��������
*/
auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  auto it = request_queue.begin();
  while (it->txn_id_ != txn_id) {           //��������������֪���ҵ���Ӧ�������
    ++it;
  }

  request_queue.erase(it);                  //��������в�����Ӧ����������
  cv.notify_all();                          //������������
  txn->GetSharedLockSet()->erase(rid);      //������Ķ���������ɾ������
  txn->GetExclusiveLockSet()->erase(rid);   //�������д��������ɾ������
  return true;
}

}  // namespace bustub
