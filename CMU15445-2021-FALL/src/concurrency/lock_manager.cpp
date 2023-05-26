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


//读锁
//在LockShared中，事务txn请求元组ID为rid的读锁
auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {

  // 进行前置判断，当事务的状态为ABORT时，直接返回假；
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // 如当前的事务状态为锁收缩时，调用获取锁函数将导致事务ABORT并抛出异常；
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // 当事务的隔离级别的READ_UNCOMMITTED时，其不应获取读锁，尝试获取读锁将导致ABORT并抛出异常。
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  /*
      如前置判断通过，则将当前事务状态置为GROWING，并获得互斥锁保护锁管理器的数据结构。
      然后，获取对应元组ID的锁请求队列及其相关成员，并将当前事务的锁请求加入队列。在这
      里txn_table_为保存<事务ID、事务>的二元组。需要注意，在该请求被加入队列时将就应当
      调用GetSharedLockSet()将该元组ID加入事务的持有锁集合，使得该锁被杀死时能将该请求
      从队列中删除。

      为了避免死锁，事务需要检查当前队列是否存在使得其阻塞的锁请求，如存在则判断当前事
      务的优先级是否高于该请求的事务，如是则杀死该事务；如非则将can_grant置为false表示
      事务将被阻塞。如该事务杀死了任何其他事务，则通过锁请求队列的条件变量cv唤醒其他等
      待锁的事务，使得被杀死的事务可以退出请求队列。
  */
  txn->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];              //通过rid获得锁请求队列
  auto &request_queue = lock_request_queue.request_queue_;  //获得请求队列
  auto &cv = lock_request_queue.cv_;                        //获得用于通知此rid上被阻止的事务
  auto txn_id = txn->GetTransactionId();                    //获得事务ID
  request_queue.emplace_back(txn_id, LockMode::SHARED);     //将当前事务的锁请求加入队列
  txn->GetSharedLockSet()->emplace(rid);                    //插入此事务所持有的共享锁定元组集
  txn_table_[txn_id] = txn;                                 //插入事务表
  //Wound Wait : Kill all low priority transaction
  bool can_grant = true;
  bool is_kill = false;
  for (auto &request : request_queue) {                     //查询请求队列
    if (request.lock_mode_ == LockMode::EXCLUSIVE) {        //若当前元组锁为写锁，读锁可重复加，不需杀死
      if (request.txn_id_ > txn_id) {                       //当前事务的优先级高于该请求的优先级，杀死事务
        txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      } else {                                              //如果不是事务阻塞
        can_grant = false;
      }
    }
    if (request.txn_id_ == txn_id) {        //当遍历到最后的事务
      request.granted_ = can_grant;         //设置是否阻塞
      break;
    }
  }

  //如该事务杀死了任何其他事务，则通过锁请求队列的条件变量cv唤醒其他等待锁的事务，
  //使得被杀死的事务可以退出请求队列。
  if (is_kill) {    
    cv.notify_all();
  }
  //Wait the lock
  while (!can_grant) {      //如存在阻塞该事务的其他事务，且该事务不能将其杀死，则进入循环等待锁。
    for (auto &request : request_queue) {

      //队列中在该事务之前的锁请求是否存在活写锁（状态不为ABORT），如是则继续阻塞，
      if (request.lock_mode_ == LockMode::EXCLUSIVE &&      
          txn_table_[request.txn_id_]->GetState() != TransactionState::ABORTED) {
        break;
      }

      //如能遍历到最后，说明前方无阻塞事务，则将该请求的granted_置为真，并返回。
      if (request.txn_id_ == txn_id) {  
        can_grant = true;
        request.granted_ = true;
      }
    }

    //如果遍历完还是存在阻塞事务，事务调用条件变量cv的wait阻塞自身，并原子地释放锁。
    if (!can_grant) {
      cv.wait(lk);
    }

    //检查该事务是否被杀死，如是则抛出异常
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return true;
}


//写锁
//LockExclusive使得事务txn尝试获得元组ID为rid的元组写锁。
auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
//进行前置检查，如当前事务状态为ABORT返回假
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
// 如当前锁在收缩阶段，则将其状态置为ABORT并抛出异常
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }


  /*
  更新事务状态，获取锁请求队列，并将该请求插入队列，以及将该锁加入事务的拥有锁集合。
  
  查询是否有将该事务锁请求阻塞的请求，当获取写锁时，队列中的任何一个锁请求都将造成其
  阻塞，当锁请求的事务优先级低时，将其杀死。如存在不能杀死的请求，则该事务将被阻塞。
  当杀死了任一事务时，将唤醒该锁等待队列的所有事务。
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
  等待锁可用，每当事务被唤醒时，检查其是否被杀死，如被杀死则抛出异常；如未被杀死，
  则检查队列前是否有任意未被杀死的锁请求，如没有则获得锁并将锁请求granted_置为真。
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

//LockUpgrade用于将当前事务txn所拥有的元组ID为rid的读锁升级为写锁。
auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  /*
  判断当前事务是否被杀死，以及该元组的锁请求序列是否已经存在等待升级锁的其他事务，
  如是则杀死事务并抛出异常。如通过检验，则将当前锁请求队列的upgrading_置为当前事务
  ID，以提示该队列存在一个等待升级锁的事务。
  */
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];      //获取该元组锁请求列表
  if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {    //升级事务的txn_id（如果有）
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  // 将当前锁请求队列的upgrading_置为当前事务ID，以提示该队列存在一个等待升级锁的事务。
  lock_request_queue.upgrading_ = txn_id;   
  bool can_grant = false;
  /*
  在Wound Wait中并未提及有关更新锁的行为，在这里将其每次唤醒尝试升级锁视为一次写锁获取，
  即每次其尝试升级锁时都将杀死队列前方将其阻塞的事务。

  其具体方法为，每次事务被唤醒时，先检查其是否被杀死，然后遍历锁请求队列在其前方的请求，
  如其优先级较低则将其杀死，如其优先级较高则将can_grant置为假，示意其将在之后被阻塞。
  如杀死任意一个事务，则唤醒其他事务。如can_grant为假则阻塞事务，如为真则更新锁请求的
  lock_mode_并将upgrading_初始化。
  
  当升级成功时，更新事务的拥有锁集合。
  */
  while (!can_grant) {
    auto it = request_queue.begin();
    auto target = it;
    can_grant = true;
    bool is_kill = false;
    while (it != request_queue.end() && it->granted_) {
      if (it->txn_id_ == txn_id) {              //为本次事务，跳过
        target = it;
      } else if (it->txn_id_ > txn_id) {        //如其优先级较低则将其杀死
        txn_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      } else {                      
        can_grant = false;         //如其优先级较高则将can_grant置为假，示意其将在之后被阻塞。
      }
      ++it;
    }
    if (is_kill) {
      cv.notify_all();
    }
    if (!can_grant) {   //如果阻塞
      cv.wait(lk);      //挂起
    } else {
      target->lock_mode_ = LockMode::EXCLUSIVE;         //否则升级为写锁
      lock_request_queue.upgrading_ = INVALID_TXN_ID;   //设置更新完毕，取消升级锁标志
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  //当升级成功时，更新事务的拥有锁集合。
  txn->GetSharedLockSet()->erase(rid);      //擦除读锁记录
  txn->GetExclusiveLockSet()->emplace(rid); //添加写锁记录
  return true;
}


//解锁
//Unlock函数使得事务txn释放元组ID为rid元组上的锁。
/*
需要注意，当事务隔离级别为READ_COMMIT时，事务获得的读锁将在使用完毕后立即释放，
因此该类事务不符合2PL规则，为了程序的兼容性，在这里认为READ_COMMIT事务在COMMIT或ABORT
之前始终保持GROWING状态，对于其他事务，将在调用Unlock时转变为SHRINKING状态。在释放锁时
，遍历锁请求对列并删除对应事务的锁请求，然后唤醒其他事务，并在事务的锁集合中删除该锁。
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
  while (it->txn_id_ != txn_id) {           //遍历锁队列请求，知道找到对应事务的锁
    ++it;
  }

  request_queue.erase(it);                  //锁请求队列擦除对应事务锁请求
  cv.notify_all();                          //唤醒其他事务
  txn->GetSharedLockSet()->erase(rid);      //在事务的读锁集合中删除该锁
  txn->GetExclusiveLockSet()->erase(rid);   //在事务的写锁集合中删除该锁
  return true;
}

}  // namespace bustub
