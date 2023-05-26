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
  auto exec_ctx = GetExecutorContext();                             //���ִ����
  Transaction *txn = exec_ctx_->GetTransaction();                   //�������
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();  //������������
  LockManager *lock_mgr = exec_ctx->GetLockManager();               //�����������

  Tuple src_tuple;  
  while (child_executor_->Next(&src_tuple, rid)) {                      //��ԴΪ�����ƻ��ڵ�
    *tuple = this->GenerateUpdatedTuple(src_tuple);                     //���ɸ���Ԫ��
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {  //�������ȼ���Ϊ���ظ���
      if (!lock_mgr->LockExclusive(txn, *rid)) {                        //��д��
        txn_mgr->Abort(txn);
      }
    } else {                        
      if (!lock_mgr->LockUpgrade(txn, *rid)) {                          //��������
        txn_mgr->Abort(txn);
      }
    }
    if (table_info_->table_->UpdateTuple(*tuple, *rid, txn)) {          //����Ԫ�飬����page
      for (auto indexinfo : indexes_) {     //����������RID��Ϊ��ִ�������Ԫ���RID
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
UpdateExecutor::Next�У�����GenerateUpdatedTuple������ԴԪ�����Ϊ��Ԫ�飬�ڸ�������ʱ��
ɾ��������ԴԪ���Ӧ������������¼������������Ԫ���Ӧ��������¼��
*/
Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();            //��ø�������,
  Schema schema = table_info_->schema_;                         //��ñ�ģʽ
  uint32_t col_count = schema.GetColumnCount();                 //�������
  std::vector<Value> values;

  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {        //���������������Ԫ���������һ�з������ֵ����
      values.emplace_back(src_tuple.GetValue(&schema, idx));    //���src_tuple��Ӧֵ��ӵ�values�У������ֲ���
    } else {
      const UpdateInfo info = update_attrs.at(idx);     //��ȡ���²���ģʽ
      Value val = src_tuple.GetValue(&schema, idx);     //��ȡԪ��ֵ
      switch (info.type_) {     //ѡ����²���ģʽ
        case UpdateType::Add:   //�����²���ΪAdd
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:   //�����²���ΪSet
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
