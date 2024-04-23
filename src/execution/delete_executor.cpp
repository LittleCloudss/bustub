//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  if (!this->GetExecutorContext()->GetTransaction()->IsTableExclusiveLocked(plan_->TableOid()) &&
      !this->GetExecutorContext()->GetTransaction()->IsTableSharedIntentionExclusiveLocked(plan_->TableOid())) {
    try {
      bool ret = this->GetExecutorContext()->GetLockManager()->LockTable(
          this->GetExecutorContext()->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
      if (!ret) {
        throw ExecutionException("Delete can't get table lock");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Delete can't get table lock because transaction abort." + e.GetInfo());
    }
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (returned_) {
    return false;
  }
  auto table = this->GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto index_vector = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table->name_);
  Tuple tmp_tuple;
  RID tmp_rid;
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    try {
      bool ret = this->GetExecutorContext()->GetLockManager()->LockRow(
          this->GetExecutorContext()->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), tmp_rid);
      if (!ret) {
        throw ExecutionException("Delete can't get row lock");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Delete can't get row lock because transaction abort." + e.GetInfo());
    }
    if (!table->table_->MarkDelete(tmp_rid, this->exec_ctx_->GetTransaction())) {
      continue;
    }
    delete_num_++;
    for (auto index : index_vector) {
      index->index_->DeleteEntry(
          tmp_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs()), tmp_rid,
          this->exec_ctx_->GetTransaction());
      this->GetExecutorContext()->GetTransaction()->GetIndexWriteSet()->emplace_back(
          tmp_rid, table->oid_, WType::DELETE, tmp_tuple, index->index_oid_, this->exec_ctx_->GetCatalog());
    }
  }
  std::vector<Value> values;
  values.reserve(1);
  values.emplace_back(TypeId::INTEGER, delete_num_);
  *tuple = Tuple{values, &this->GetOutputSchema()};
  returned_ = true;
  return true;
}

}  // namespace bustub
