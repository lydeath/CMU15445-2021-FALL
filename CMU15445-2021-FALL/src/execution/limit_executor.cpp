//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

/*
LimitExecutor用于限制输出元组的数量，其计划节点中定义了具体的限制数量。其Init()应当调用子计
划节点的Init()方法，并重置当前限制数量；Next()方法则将子计划节点的元组返回，直至限制数量为0。
*/
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  limit_ = plan_->GetLimit();
}

void LimitExecutor::Init() {
  child_executor_->Init();
  limit_ = plan_->GetLimit();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (limit_ == 0 || !child_executor_->Next(tuple, rid)) {
    return false;
  }
  --limit_;
  return true;
}

}  // namespace bustub
