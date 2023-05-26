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
    *SeqScanPlanNode表示一种顺序表扫描操作。
    *它标识了一个要扫描的表和一个可选谓词。
    * 
    * Parm  ExecutorContext存储运行执行器所需的所有上下文。
    * 
    * Parm  SeqScanPlanNode表示一种顺序表扫描操作。
    * 它标识了一个要扫描的表和一个可选谓词。
    * 
    * SeqScanExecutor执行顺序扫描操作，其通过Next()方法顺序遍历其对应表中的所有元组，并将元组返回至调用者。在bustub中，所有与表有关的信息被包含在TableInfo中：
    * 
    
*/

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),             //RID()  为给定的页面标识符和插槽号创建一个新的记录标识符
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {

}



// 通过列名，偏移量判断模式是否相同
bool SeqScanExecutor::SchemaEqual(const Schema *table_schema, const Schema *output_schema) {
    auto table_colums = table_schema->GetColumns();     // 获得表输入列数组
    auto output_colums = output_schema->GetColumns();   // 获得表输出列数组
    if (table_colums.size() != output_colums.size()) {  // 如果两数组大小不一，则模式不相等
        return false;
    }

    int col_size = table_colums.size();             // 获得列大小
    uint32_t offset1;                               // 偏移量1
    uint32_t offset2;                               // 偏移量2
    std::string name1;                              // 姓名1
    std::string name2;                              // 姓名2
    for (int i = 0; i < col_size; i++) {            //遍历列
        offset1 = table_colums[i].GetOffset();
        offset2 = output_colums[i].GetOffset();
        name1 = table_colums[i].GetName();
        name2 = table_colums[i].GetName();
        if (name1 != name2 || offset1 != offset2) { //如果偏移量或列名有一处不相等
            return false;                           //返回false
        }
    }
    return true;
}

void SeqScanExecutor::Init() { 
    table_oid_t oid = plan_->GetTableOid();                             //从plan中获取oid
    table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);               //通过索引表用oid获取对应表
    iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());    //获取起始迭代器
    end_ = table_info_->table_->End();                                  //获取结束迭代器

    auto output_schema = plan_->OutputSchema();                         //获取表输出模式
    auto table_schema = table_info_->schema_;                           //获取表输入模式
    is_same_schema_ = SchemaEqual(&table_schema, output_schema);        //判断是否相同

    // 可重复读：给所有元组加上读锁，事务提交后再解锁
    auto transaction = exec_ctx_->GetTransaction();                             //获得事务
    auto lockmanager = exec_ctx_->GetLockManager();                             //获得锁管理器
    if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {  //如果隔离级别为可重复读
        auto iter = table_info_->table_->Begin(exec_ctx_->GetTransaction());    //遍历表，给每行元组都上读锁
        while (iter != table_info_->table_->End()) {
            lockmanager->LockShared(transaction, iter->GetRid());
            ++iter;
        }
    }
}


//在Init()中，执行计划节点所需的初始化操作，在这里重新设定表的迭代器，使得查询计划可以重新遍历表：
void SeqScanExecutor::Init() {
  table_oid_t oid = plan_->GetTableOid();                // 获得应该扫描表的标识符
  table_info_ = exec_ctx_->GetCatalog()->GetTable(oid);  // 按OID查询表元数据
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());  // table_：指向表堆的拥有指针  transaction 与此执行器上下文关联的事务上下文
  end_ = table_info_->table_->End();

  auto output_schema = plan_->OutputSchema();                   //输出模式
  auto table_schema = table_info_->schema_;                     //表模式
  is_same_schema_ = SchemaEqual(&table_schema, output_schema);

  //可重复读：给所有元组加上读锁，事务提交后再解锁
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

//在Next() 中，计划节点遍历表，并通过输入参数返回元组，当遍历结束时返回假：
//在这里，通过迭代器iter_访问元组，当计划节点谓词predicate非空时，通过predicate的Evaluate方法
//评估当前元组是否满足谓词，如满足则返回，否则遍历下一个元组。值得注意的是，表中的元组应当以
//out_schema的模式被重组。在bustub中，所有查询计划节点的输出元组均通过out_schema中各列Column
//的ColumnValueExpression中的各种“Evaluate”方法构造，如Evaluate、EvaluateJoin、
//EvaluateAggregate。
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    
    const AbstractExpression *predicate = plan_->GetPredicate();    //获得谓词
    const Schema *output_schema = plan_->OutputSchema();            //获取表输出模式
    Schema table_schema = table_info_->schema_;                     //获取表输入模式
    auto exec_ctx = GetExecutorContext();                           //获取该操作的执行器上下文
    Transaction *transaction = exec_ctx->GetTransaction();          //获取事务
    LockManager *lockmanager = exec_ctx->GetLockManager();          //获取锁
    bool res;

    while (iter_ != end_) {
        // 读已提交：读元组时加上读锁，读完后立即释放
        if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
          lockmanager->LockShared(transaction, iter_->GetRid());
        }
        auto p_tuple = &(*iter);    // 获取指向元组的指针；
        res = true;
        if (predicate != nullptr) {
            res = predicate->Evaluate(p_tuple, &table_schema).GetAs<bool>();    //查看经过谓词过滤后是否还有数据存在
        }                                                                       //如果存在继续处理，如果不存在返回false
        if (res) {                                  
            if (!is_same_schema_) {             //如果表模式与输出模式不一致,改变元组数据结构
                TupleSchemaTranformUseEvaluate(p_tuple, &table_schema, tuple, output_schema);
            } else {                            //如果一致，不进行处理
                *tuple = *p_tuple;
            }
            *rid = p_tuple->GetRid(); 
        }
        if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
            lockmanager->Unlock(transaction, iter_->GetRid());
        }
        ++iter_;    /// 指向下一位置后再返回
        if (res) {
            return true;
        }
    }
    return false;
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {

  //用于测试元组的谓词；只有当元组的计算结果为true时，才应返回元组
  const AbstractExpression *predicate = plan_->GetPredicate();  
  //返回此计划节点输出的架构
  const Schema *output_schema = plan_->OutputSchema();
  Schema table_schema = table_info_->schema_;
  auto exec_ctx = GetExecutorContext();
  Transaction *transaction = exec_ctx->GetTransaction();
  //TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
  LockManager *lockmanager = exec_ctx->GetLockManager();
  bool res;
  
  while (iter_ != end_) {
      // 读已提交：读元组时加上读锁，读完后立即释放
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->LockShared(transaction, iter_->GetRid());
    }

    auto p_tuple = &(*iter_);  // 获取指向元组的指针；
    res = true;
    if (predicate != nullptr) {
      res = predicate->Evaluate(p_tuple, &table_schema).GetAs<bool>();  //如果有数据，返回true
    }

    if (res) {
      if (!is_same_schema_) {   //如果表模式与输出模式不一致,改变元组数据结构
        TupleSchemaTranformUseEvaluate(p_tuple, &table_schema, tuple, output_schema);
      } else {                  //如果一致，不进行处理
        *tuple = *p_tuple;
      }
      *rid = p_tuple->GetRid(); //返回行元组的ID
    }
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->Unlock(transaction, iter_->GetRid());
    }
    ++iter_;    // 指向下一位置后再返回
    if (res) {
      return true;
    }
  }
  return false;
}

void SeqScanExecutor::TupleSchemaTranformUseEvaluate(const Tuple *table_tuple, const Schema *table_schema,
                                                     Tuple *dest_tuple, const Schema *dest_schema) {
  auto colums = dest_schema->GetColumns();  //架构中的所有列
  std::vector<Value> dest_value;
  dest_value.reserve(colums.size());

  for (const auto &col : colums) {
    dest_value.emplace_back(col.GetExpr()->Evaluate(table_tuple, table_schema));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);
}

}  // namespace bustub
