//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_(this->exec_ctx_->GetCatalog()
                 ->GetTable(this->exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->table_name_)
                 ->table_.get()),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          this->exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())),
      itr_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (itr_ == tree_->GetEndIterator()) {
    return false;
  }
  *rid = (*itr_).second;

  table_->GetTuple(*rid, tuple, this->exec_ctx_->GetTransaction());
  ++itr_;
  return true;
}

}  // namespace bustub
