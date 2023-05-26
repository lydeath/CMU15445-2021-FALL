//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <iostream>
namespace bustub {

/*
    *SeqScanPlanNode��ʾһ��˳���ɨ�������
    *����ʶ��һ��Ҫɨ��ı��һ����ѡν�ʡ�
    * 
    * Parm  ExecutorContext�洢����ִ������������������ġ�
    * 
    * Parm  SeqScanPlanNode��ʾһ��˳���ɨ�������
    * ����ʶ��һ��Ҫɨ��ı��һ����ѡν�ʡ�
    * 
    * SeqScanExecutorִ��˳��ɨ���������ͨ��Next()����˳��������Ӧ���е�����Ԫ�飬����Ԫ�鷵���������ߡ���bustub�У���������йص���Ϣ��������TableInfo�У�
    * 
    
*/

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),             //RID()  Ϊ������ҳ���ʶ���Ͳ�ۺŴ���һ���µļ�¼��ʶ��
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {

}



// ͨ��������ƫ�����ж�ģʽ�Ƿ���ͬ
bool SeqScanExecutor::SchemaEqual(const Schema *table_schema, const Schema *output_schema) {
    auto table_colums = table_schema->GetColumns();     // ��ñ�����������
    auto output_colums = output_schema->GetColumns();   // ��ñ����������
    if (table_colums.size() != output_colums.size()) {  // ����������С��һ����ģʽ�����
        return false;
    }

    int col_size = table_colums.size();             // ����д�С
    uint32_t offset1;                               // ƫ����1
    uint32_t offset2;                               // ƫ����2
    std::string name1;                              // ����1
    std::string name2;                              // ����2
    for (int i = 0; i < col_size; i++) {            //������
        offset1 = table_colums[i].GetOffset();
        offset2 = output_colums[i].GetOffset();
        name1 = table_colums[i].GetName();
        name2 = table_colums[i].GetName();
        if (name1 != name2 || offset1 != offset2) { //���ƫ������������һ�������
            return false;                           //����false
        }
    }
    return true;
}

void SeqScanExecutor::Init() { 
    table_oid_t oid = plan_->GetTableOid();                             //��plan�л�ȡoid
    table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);               //ͨ����������oid��ȡ��Ӧ��
    iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());    //��ȡ��ʼ������
    end_ = table_info_->table_->End();                                  //��ȡ����������

    auto output_schema = plan_->OutputSchema();                         //��ȡ�����ģʽ
    auto table_schema = table_info_->schema_;                           //��ȡ������ģʽ
    is_same_schema_ = SchemaEqual(&table_schema, output_schema);        //�ж��Ƿ���ͬ

    // ���ظ�����������Ԫ����϶����������ύ���ٽ���
    auto transaction = exec_ctx_->GetTransaction();                             //�������
    auto lockmanager = exec_ctx_->GetLockManager();                             //�����������
    if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {  //������뼶��Ϊ���ظ���
        auto iter = table_info_->table_->Begin(exec_ctx_->GetTransaction());    //��������ÿ��Ԫ�鶼�϶���
        while (iter != table_info_->table_->End()) {
            lockmanager->LockShared(transaction, iter->GetRid());
            ++iter;
        }
    }
}


//��Init()�У�ִ�мƻ��ڵ�����ĳ�ʼ�������������������趨��ĵ�������ʹ�ò�ѯ�ƻ��������±�����
void SeqScanExecutor::Init() {
  table_oid_t oid = plan_->GetTableOid();                // ���Ӧ��ɨ���ı�ʶ��
  table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);  // ��OID��ѯ��Ԫ����
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());  // table_��ָ���ѵ�ӵ��ָ��  transaction ���ִ���������Ĺ���������������
  end_ = table_info_->table_->End();

  auto output_schema = plan_->OutputSchema();                   //���ģʽ
  auto table_schema = table_info_->schema_;                     //��ģʽ
  is_same_schema_ = SchemaEqual(&table_schema, output_schema);

  //���ظ�����������Ԫ����϶����������ύ���ٽ���
  auto transaction = exec_ctx_->GetTransaction();
  auto lockmanager = exec_ctx_->GetLockManager();
  if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    auto iter = table_info_->table_->Begin(exec_ctx_->GetTransaction());
    while (iter != table_info_->table_->End()) {
      lockmanager->LockShared(transaction, iter->GetRid());
      ++iter;
    }
  }
}

//��Next() �У��ƻ��ڵ��������ͨ�������������Ԫ�飬����������ʱ���ؼ٣�
//�����ͨ��������iter_����Ԫ�飬���ƻ��ڵ�ν��predicate�ǿ�ʱ��ͨ��predicate��Evaluate����
//������ǰԪ���Ƿ�����ν�ʣ��������򷵻أ����������һ��Ԫ�顣ֵ��ע����ǣ����е�Ԫ��Ӧ����
//out_schema��ģʽ�����顣��bustub�У����в�ѯ�ƻ��ڵ�����Ԫ���ͨ��out_schema�и���Column
//��ColumnValueExpression�еĸ��֡�Evaluate���������죬��Evaluate��EvaluateJoin��
//EvaluateAggregate��
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    
    const AbstractExpression *predicate = plan_->GetPredicate();    //���ν��
    const Schema *output_schema = plan_->OutputSchema();            //��ȡ�����ģʽ
    Schema table_schema = table_info_->schema_;                     //��ȡ������ģʽ
    auto exec_ctx = GetExecutorContext();                           //��ȡ�ò�����ִ����������
    Transaction *transaction = exec_ctx->GetTransaction();          //��ȡ����
    LockManager *lockmanager = exec_ctx->GetLockManager();          //��ȡ��
    bool res;

    while (iter_ != end_) {
        // �����ύ����Ԫ��ʱ���϶���������������ͷ�
        if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
          lockmanager->LockShared(transaction, iter_->GetRid());
        }
        auto p_tuple = &(*iter);    // ��ȡָ��Ԫ���ָ�룻
        res = true;
        if (predicate != nullptr) {
            res = predicate->Evaluate(p_tuple, &table_schema).GetAs<bool>();    //�鿴����ν�ʹ��˺��Ƿ������ݴ���
        }                                                                       //������ڼ���������������ڷ���false
        if (res) {                                  
            if (!is_same_schema_) {             //�����ģʽ�����ģʽ��һ��,�ı�Ԫ�����ݽṹ
                TupleSchemaTranformUseEvaluate(p_tuple, &table_schema, tuple, output_schema);
            } else {                            //���һ�£������д���
                *tuple = *p_tuple;
            }
            *rid = p_tuple->GetRid(); 
        }
        if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
            lockmanager->Unlock(transaction, iter_->GetRid());
        }
        ++iter_;    /// ָ����һλ�ú��ٷ���
        if (res) {
            return true;
        }
    }
    return false;
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {

  //���ڲ���Ԫ���ν�ʣ�ֻ�е�Ԫ��ļ�����Ϊtrueʱ����Ӧ����Ԫ��
  const AbstractExpression *predicate = plan_->GetPredicate();  
  //���ش˼ƻ��ڵ�����ļܹ�
  const Schema *output_schema = plan_->OutputSchema();
  Schema table_schema = table_info_->schema_;
  auto exec_ctx = GetExecutorContext();
  Transaction *transaction = exec_ctx->GetTransaction();
  //TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
  LockManager *lockmanager = exec_ctx->GetLockManager();
  bool res;
  
  while (iter_ != end_) {
      // �����ύ����Ԫ��ʱ���϶���������������ͷ�
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->LockShared(transaction, iter_->GetRid());
    }

    auto p_tuple = &(*iter_);  // ��ȡָ��Ԫ���ָ�룻
    res = true;
    if (predicate != nullptr) {
      res = predicate->Evaluate(p_tuple, &table_schema).GetAs<bool>();  //��������ݣ�����true
    }

    if (res) {
      if (!is_same_schema_) {   //�����ģʽ�����ģʽ��һ��,�ı�Ԫ�����ݽṹ
        TupleSchemaTranformUseEvaluate(p_tuple, &table_schema, tuple, output_schema);
      } else {                  //���һ�£������д���
        *tuple = *p_tuple;
      }
      *rid = p_tuple->GetRid(); //������Ԫ���ID
    }
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->Unlock(transaction, iter_->GetRid());
    }
    ++iter_;    // ָ����һλ�ú��ٷ���
    if (res) {
      return true;
    }
  }
  return false;
}

void SeqScanExecutor::TupleSchemaTranformUseEvaluate(const Tuple *table_tuple, const Schema *table_schema,
                                                     Tuple *dest_tuple, const Schema *dest_schema) {
  auto colums = dest_schema->GetColumns();  //�ܹ��е�������
  std::vector<Value> dest_value;
  dest_value.reserve(colums.size());

  for (const auto &col : colums) {
    dest_value.emplace_back(col.GetExpr()->Evaluate(table_tuple, table_schema));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);
}

}  // namespace bustub
