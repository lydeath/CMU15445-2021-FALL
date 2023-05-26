//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/logger.h"
#include "execution/executors/insert_executor.h"
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
    table_oid_t oid = plan->TableOid();                   //��ò���Ԫ��ı�ı�ʶ��
    table_info_ = exec_ctx->GetCatalog()->GetTable(oid);  //ͨ��Ŀ¼��ȡ����Ϣ
    is_raw_ = plan->IsRawInsert();                        //�������ֱ�Ӳ��뷵��true, ����������Ӽƻ�����false
    if (is_raw_) {
        size_ = plan->RawValues().size();                   //RawValues  Ҫ�����ԭʼֵ
    }
    indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);   //��ȡ�ɡ�table_name����ʶ�ı����������
}

//��Ԫ����ԴΪ�����ƻ��ڵ�ʱ��ִ�ж�Ӧ�ƻ��ڵ��Init(����)
void InsertExecutor::Init() {
    if (!is_raw_) {               // ��ʼ���Ӽƻ��������������
        child_executor_->Init();    //�Ӽƻ���ʼ��
    }
}

/*
��Ҫע�⣬Insert�ڵ㲻Ӧ��������κ�Ԫ�飬���������Ƿ��ؼ٣������еĲ��������Ӧ����
һ��Next�б�ִ����ɡ�����ԴΪ�Զ����Ԫ������ʱ�����ݱ�ģʽ�����Ӧ��Ԫ�飬�������
�У�����ԴΪ�����ƻ��ڵ�ʱ��ͨ���ӽڵ��ȡ����Ԫ�鲢������ڲ�������У�Ӧ��ʹ��
InsertEntry���±��е�����������InsertEntry�Ĳ���Ӧ��KeyFromTuple�������졣
*/

//��Next() �У����ݲ�ͬ��Ԫ����Դʵʩ��ͬ�Ĳ�����ԣ�
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();                             // ���ִ����
  Transaction *txn = exec_ctx_->GetTransaction();                   // �������
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();  // ������������
  LockManager *lock_mgr = exec_ctx->GetLockManager();               // �����������

  Tuple tmp_tuple;  // Ԫ��
  RID tmp_rid;      // ��¼��ʶ����������ű���ֵ

    if (is_raw_) {
    for (uint32_t idx = 0; idx < size_; idx++) {
        const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);      // Ҫ�����ض�������ԭʼֵ
        tmp_tuple = Tuple(raw_value, &table_info_->schema_);                // �Զ����Ԫ��������ݱ�ģʽ�����Ӧ��Ԫ�� 
        if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {   // ����Ԫ��      ��ʱtmp_rid�ű���ֵ
            if (!lock_mgr->LockExclusive(txn, tmp_rid)) {                   // ���û����д��
            txn_mgr->Abort(txn);                                            // ��������ز��
            }                                       //���������б�
            for (auto indexinfo : indexes_) {       //���������б�
                indexinfo->index_->InsertEntry(     //��������
                    tmp_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()), //���Զ����Ԫ�������������ģʽ�����Ӧ������  
                    tmp_rid, txn);
            IndexWriteRecord iwr(*rid, table_info_->oid_, WType::INSERT, *tuple, *tuple, indexinfo->index_oid_,  // ������д����ص���Ϣ��
                                    exec_ctx->GetCatalog());
            txn->AppendIndexWriteRecord(iwr);  // ������д���¼��ӵ�����д�뼯�С�
            }
        }
    }
    return false;
    }
    while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {  // ��ԴΪ�����ƻ��ڵ�
    if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {  // ����������ȼ���Ϊ���ظ���
        if (!lock_mgr->LockExclusive(txn, *rid)) {                      // ��д��
        txn_mgr->Abort(txn);
        }
    } else {
        if (!lock_mgr->LockUpgrade(txn, *rid)) {                        // д������
            txn_mgr->Abort(txn);
        }
    }
    for (auto indexinfo : indexes_) {  // ��������
        indexinfo->index_->InsertEntry(
            tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
            tmp_rid, txn);
            txn->GetIndexWriteSet()->emplace_back(tmp_rid, table_info_->oid_, WType::INSERT, tmp_tuple, tmp_tuple,
                                                    indexinfo->index_oid_, exec_ctx->GetCatalog());
            }
        }
    }
    return false;
}



}  // namespace bustub
