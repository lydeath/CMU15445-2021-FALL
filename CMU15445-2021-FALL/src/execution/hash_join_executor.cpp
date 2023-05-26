//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(left_child.release()), right_child_(right_child.release()) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hash_map_.clear();
  output_buffer_.clear();
  Tuple left_tuple;
  const Schema *left_schema = left_child_->GetOutputSchema();
  RID rid;
  while (left_child_->Next(&left_tuple, &rid)) {
    HashJoinKey left_key;
    left_key.value_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema);       //设置左表的hashjoinkey
    hash_map_.emplace(left_key, left_tuple);    //加入hash_map_映射<<HashJoinKey, Tuple>>
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!output_buffer_.empty()) {        //如果已存在结果，弹出结果，返回true
    *tuple = output_buffer_.back();
    output_buffer_.pop_back();
    return true;
  }
  Tuple right_tuple;
  const Schema *left_schema = left_child_->GetOutputSchema();
  const Schema *right_schema = right_child_->GetOutputSchema();
  const Schema *out_schema = GetOutputSchema();
  while (right_child_->Next(&right_tuple, rid)) {
    HashJoinKey right_key;
    right_key.value_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_schema);   //设置右表的hashjoinkey
    auto iter = hash_map_.find(right_key);                  //在映射表中寻找对应键
    uint32_t num = hash_map_.count(right_key);              //获取存在符合键值对的总数
    for (uint32_t i = 0; i < num; ++i, ++iter) {            //遍历符合键值对
      std::vector<Value> values;
      for (const auto &col : out_schema->GetColumns()) {    //通过谓词过滤获得有效值
        values.emplace_back(col.GetExpr()->EvaluateJoin(&iter->second, left_schema, &right_tuple, right_schema));   
      } 
      output_buffer_.emplace_back(values, out_schema);      //放入结果中
    }
    if (!output_buffer_.empty()) {      //如果已存在结果，弹出结果，返回true
      *tuple = output_buffer_.back();
      output_buffer_.pop_back();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
