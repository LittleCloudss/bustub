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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(this->GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
      itr_(tree_->Begin(this->GetExecutorContext()->GetTransaction())) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (itr_ == tree_->End()) {
    return false;
  }
  *tuple = *itr_;
  *rid = itr_->GetRid();
  ++itr_;
  return true;
}

}  // namespace bustub
