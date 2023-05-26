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
    table_oid_t oid = plan->TableOid();                   //获得插入元组的表的标识符
    table_info_ = exec_ctx->GetCatalog()->GetTable(oid);  //通过目录获取表信息
    is_raw_ = plan->IsRawInsert();                        //如果我们直接插入返回true, 如果我们有子计划返回false
    if (is_raw_) {
        size_ = plan->RawValues().size();                   //RawValues  要插入的原始值
    }
    indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);   //获取由“table_name”标识的表的所有索引
}

//当元组来源为其他计划节点时，执行对应计划节点的Init(方法)
void InsertExecutor::Init() {
    if (!is_raw_) {               // 初始化子计划或者数组迭代器
        child_executor_->Init();    //子计划初始化
    }
}

/*
需要注意，Insert节点不应向外输出任何元组，所以其总是返回假，即所有的插入操作均应当在
一次Next中被执行完成。当来源为自定义的元组数组时，根据表模式构造对应的元组，并插入表
中；当来源为其他计划节点时，通过子节点获取所有元组并插入表。在插入过程中，应当使用
InsertEntry更新表中的所有索引，InsertEntry的参数应由KeyFromTuple方法构造。
*/

//在Next() 中，根据不同的元组来源实施不同的插入策略：
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();                             // 获得执行器
  Transaction *txn = exec_ctx_->GetTransaction();                   // 获得事务
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();  // 获得事务管理器
  LockManager *lock_mgr = exec_ctx->GetLockManager();               // 获得锁管理器

  Tuple tmp_tuple;  // 元组
  RID tmp_rid;      // 记录标识符，插入表后才被赋值

    if (is_raw_) {
    for (uint32_t idx = 0; idx < size_; idx++) {
        const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);      // 要插入特定索引的原始值
        tmp_tuple = Tuple(raw_value, &table_info_->schema_);                // 自定义的元祖数组根据表模式构造对应的元组 
        if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {   // 插入元组      此时tmp_rid才被赋值
            if (!lock_mgr->LockExclusive(txn, tmp_rid)) {                   // 如果没锁上写锁
            txn_mgr->Abort(txn);                                            // 否则事务夭折
            }                                       //更新索引列表
            for (auto indexinfo : indexes_) {       //遍历索引列表
                indexinfo->index_->InsertEntry(     //插入索引
                    tmp_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()), //将自定义的元祖数组根据索引模式构造对应的数组  
                    tmp_rid, txn);
            IndexWriteRecord iwr(*rid, table_info_->oid_, WType::INSERT, *tuple, *tuple, indexinfo->index_oid_,  // 跟踪与写入相关的信息。
                                    exec_ctx->GetCatalog());
            txn->AppendIndexWriteRecord(iwr);  // 将索引写入记录添加到索引写入集中。
            }
        }
    }
    return false;
    }
    while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {  // 来源为其他计划节点
    if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {  // 如果事务隔离等级不为可重复读
        if (!lock_mgr->LockExclusive(txn, *rid)) {                      // 上写锁
        txn_mgr->Abort(txn);
        }
    } else {
        if (!lock_mgr->LockUpgrade(txn, *rid)) {                        // 写锁升级
            txn_mgr->Abort(txn);
        }
    }
    for (auto indexinfo : indexes_) {  // 更新索引
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
