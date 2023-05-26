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

//AggregationExecutorʵ�־ۺϲ�������ԭ��Ϊʹ�ù�ϣ�����оۺϼ���ͬ��Ԫ��ӳ����һ���Դ�ͳ�����оۺϼ�Ԫ��ľۺ���Ϣ��
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(child.release()),
      hash_table_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(hash_table_.Begin()) {}

//��Init()�У������Ӽƻ��ڵ��Ԫ�飬��������ϣ���������ڱ����ù�ϣ��ĵ�������InsertCombine����ǰ�ۺϼ���ͳ����Ϣ���£�
void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    hash_table_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  iter_ = hash_table_.Begin();
}

//��Next()�У�ʹ�õ�����������ϣ�������ν�ʣ���ʹ��ν�ʵ�EvaluateAggregate
//�жϵ�ǰ�ۺϼ��Ƿ����ν�ʣ��粻�������������ֱ��Ѱ�ҵ�����ν�ʵľۺϼ���
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != hash_table_.End()) {              //������ϣ��
    auto *having = plan_->GetHaving();
    const auto &key = iter_.Key().group_bys_;
    const auto &val = iter_.Val().aggregates_;
    if (having == nullptr || having->EvaluateAggregate(key, val).GetAs<bool>()) {   //�����������
      std::vector<Value> values;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateAggregate(key, val));    //������
      }
      *tuple = Tuple(values, GetOutputSchema());    //���ý��Ԫ��
      ++iter_;
      return true;
    }
    ++iter_;    //���ν�ʹ��˺󲻷�����������������
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
