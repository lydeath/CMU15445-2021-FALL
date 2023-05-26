//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(left_executor.release()),
      right_executor_(right_executor.release()) {}


//�����Init()����������е����Ӳ����������õ�����������Ԫ�����ڻ�����buffer_�С�
//��ͨ���Ӽƻ��ڵ��Next()�����õ��Ӽƻ��ڵ��Ԫ�飬ͨ��˫��ѭ������ÿһ��Ԫ����ϣ�
//���ڲ�ƻ��ڵ㷵�ؼ�ʱ������Init()ʹ���ʼ�����ڵõ��Ӽƻ��ڵ�Ԫ��������ν�ʣ�
//�����ν�ʵ�EvaluateJoin��֤���Ƿ����ν�ʡ��粻����ν�ʻ����ν�ʣ���ͨ������out_schema
//��Column��EvaluateJoin�õ����Ԫ�飬����������buffer_��
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  buffer_.clear();
  const Schema *left_schema = plan_->GetLeftPlan()->OutputSchema();     //�������ģʽ
  const Schema *right_schema = plan_->GetRightPlan()->OutputSchema();   //�ұ�����ģʽ
  const Schema *out_schema = this->GetOutputSchema();                   //�����ģʽ
  Tuple left_tuple;
  Tuple right_tuple;
  RID rid;
  while (left_executor_->Next(&left_tuple, &rid)) {     //����һ
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &rid)) { //�ұ������ĩβ
      auto *predicate = plan_->Predicate();             //���ù���ν��
      if (predicate == nullptr ||                       //���û��ν�ʻ���˺��Դ�������
          predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {    //��������
        std::vector<Value> values;
        for (const auto &col : out_schema->GetColumns()) {  //�������У����ϵ�ֵ����values�б���
          values.emplace_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
        }
        buffer_.emplace_back(values, out_schema);
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!buffer_.empty()) {
    *tuple = buffer_.back();
    buffer_.pop_back();
    return true;
  }
  return false;
}

}  // namespace bustub
