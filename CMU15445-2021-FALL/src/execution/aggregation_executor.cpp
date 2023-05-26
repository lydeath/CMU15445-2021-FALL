//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

//AggregationExecutor实现聚合操作，其原理为使用哈希表将所有聚合键相同的元组映射在一起，以此统计所有聚合键元组的聚合信息：
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(child.release()),
      hash_table_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(hash_table_.Begin()) {}

//在Init()中，遍历子计划节点的元组，并构建哈希表及设置用于遍历该哈希表的迭代器。InsertCombine将当前聚合键的统计信息更新：
void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    hash_table_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  iter_ = hash_table_.Begin();
}

//在Next()中，使用迭代器遍历哈希表，如存在谓词，则使用谓词的EvaluateAggregate
//判断当前聚合键是否符合谓词，如不符合则继续遍历直到寻找到符合谓词的聚合键。
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != hash_table_.End()) {              //遍历哈希表
    auto *having = plan_->GetHaving();
    const auto &key = iter_.Key().group_bys_;
    const auto &val = iter_.Val().aggregates_;
    if (having == nullptr || having->EvaluateAggregate(key, val).GetAs<bool>()) {   //如果符合条件
      std::vector<Value> values;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateAggregate(key, val));    //放入结果
      }
      *tuple = Tuple(values, GetOutputSchema());    //设置结果元组
      ++iter_;
      return true;
    }
    ++iter_;    //如果谓词过滤后不符合条件，继续遍历
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
