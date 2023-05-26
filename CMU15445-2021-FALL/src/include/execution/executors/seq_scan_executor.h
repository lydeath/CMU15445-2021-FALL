//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"
namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 * SeqScanExecutorִ����ִ��˳���ɨ�衣
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   * 
   *����һ���µ�SeqScanExecutorʵ����
   *@param exec_ctxִ����������
   *@param planҪִ�е�˳��ɨ��ƻ�
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:

  void TupleSchemaTranformUseEvaluate(const Tuple *table_tuple, const Schema *table_schema, Tuple *dest_tuple,
                                      const Schema *dest_schema);

  bool SchemaEqual(const Schema *table_schema, const Schema *output_schema);
  /** The sequential scan plan node to be executed */
  // Ҫִ�е�˳��ɨ��ƻ��ڵ�
  const SeqScanPlanNode *plan_;

  TableInfo *table_info_;

  TableIterator iter_;

  TableIterator end_;

  bool is_same_schema_;	//��ģʽ�����ģʽ�Ƿ�һ��
};
}  // namespace bustub
