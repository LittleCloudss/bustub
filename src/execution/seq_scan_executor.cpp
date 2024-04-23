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
      itr_(TableIterator(nullptr, RID(), nullptr)) {}

void SeqScanExecutor::Init() {
  if (this->GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!this->GetExecutorContext()->GetTransaction()->IsTableSharedLocked(plan_->GetTableOid()) &&
        !this->GetExecutorContext()->GetTransaction()->IsTableExclusiveLocked(plan_->GetTableOid()) &&
        !this->GetExecutorContext()->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->GetTableOid()) &&
        !this->GetExecutorContext()->GetTransaction()->IsTableSharedIntentionExclusiveLocked(plan_->GetTableOid())) {
      try {
        bool ret = this->GetExecutorContext()->GetLockManager()->LockTable(this->GetExecutorContext()->GetTransaction(),
                                                                           LockManager::LockMode::INTENTION_SHARED,
                                                                           plan_->GetTableOid());
        if (!ret) {
          throw ExecutionException("Seq scan can't get table lock");
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("Seq scan can't get table lock because transaction abort." + e.GetInfo());
      }
    }
  }
  itr_ = tree_->Begin(this->GetExecutorContext()->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (itr_ == tree_->End()) {
    if (this->GetExecutorContext()->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      auto row_lock = this->GetExecutorContext()->GetTransaction()->GetSharedRowLockSet()->at(plan_->GetTableOid());
      for (auto rid : row_lock) {
        this->GetExecutorContext()->GetLockManager()->UnlockRow(this->GetExecutorContext()->GetTransaction(),
                                                                plan_->GetTableOid(), rid);
      }

      this->GetExecutorContext()->GetLockManager()->UnlockTable(this->GetExecutorContext()->GetTransaction(),
                                                                plan_->GetTableOid());
    }
    return false;
  }
  *tuple = *itr_;
  *rid = itr_->GetRid();
  ++itr_;
  if (!this->GetExecutorContext()->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), *rid)) {
    try {
      if (this->GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        bool ret = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                        plan_->GetTableOid(), *rid);
        if (!ret) {
          throw ExecutionException("Seq scan can't get row lock");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Seq scan can't get row lock because transaction abort." + e.GetInfo());
    }
  }

  return true;
}

}  // namespace bustub
