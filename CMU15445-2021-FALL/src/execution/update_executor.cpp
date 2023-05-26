//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include <iostream>
#include "execution/executors/update_executor.h"
namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  auto catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() {
    child_executor_->Init(); 
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();                             //获得执行器
  Transaction *txn = exec_ctx_->GetTransaction();                   //获得事务
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();  //获得事务管理器
  LockManager *lock_mgr = exec_ctx->GetLockManager();               //获得锁管理器

  Tuple src_tuple;  
  while (child_executor_->Next(&src_tuple, rid)) {                      //来源为其他计划节点
    *tuple = this->GenerateUpdatedTuple(src_tuple);                     //生成更新元组
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {  //事务隔离等级不为可重复读
      if (!lock_mgr->LockExclusive(txn, *rid)) {                        //上写锁
        txn_mgr->Abort(txn);
      }
    } else {                        
      if (!lock_mgr->LockUpgrade(txn, *rid)) {                          //升级读锁
        txn_mgr->Abort(txn);
      }
    }
    if (table_info_->table_->UpdateTuple(*tuple, *rid, txn)) {          //更新元组，更新page
      for (auto indexinfo : indexes_) {     //更新索引，RID都为子执行器输出元组的RID
        indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);
        indexinfo->index_->InsertEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);

        IndexWriteRecord iwr(*rid, table_info_->oid_, WType::UPDATE, *tuple, src_tuple, indexinfo->index_oid_,
                             exec_ctx->GetCatalog());
        txn->AppendIndexWriteRecord(iwr);
      }
    }
  }
  return false;
}

/*
UpdateExecutor::Next中，利用GenerateUpdatedTuple方法将源元组更新为新元组，在更新索引时，
删除表中与源元组对应的所有索引记录，并增加与新元组对应的索引记录。
*/
Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();            //获得更新属性,
  Schema schema = table_info_->schema_;                         //获得表模式
  uint32_t col_count = schema.GetColumnCount();                 //获得列数
  std::vector<Value> values;

  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {        //如果遍历结束，再元组的最后额外一列放入更新值备份
      values.emplace_back(src_tuple.GetValue(&schema, idx));    //获得src_tuple对应值添加到values中，即保持不变
    } else {
      const UpdateInfo info = update_attrs.at(idx);     //获取更新操作模式
      Value val = src_tuple.GetValue(&schema, idx);     //获取元组值
      switch (info.type_) {     //选择更新操作模式
        case UpdateType::Add:   //若更新操作为Add
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:   //若更新操作为Set
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
