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
  auto table = this->GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto index_vector = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table->name_);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (!table->table_->MarkDelete(rid, this->exec_ctx_->GetTransaction())) {
      continue;
    }
    delete_num_++;
    for (auto index : index_vector) {
      index->index_->DeleteEntry(tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
                                 rid, this->exec_ctx_->GetTransaction());
    }
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (returned_) {
    return false;
  }
  std::vector<Value> values;
  values.reserve(1);
  values.emplace_back(TypeId::INTEGER, delete_num_);
  *tuple = Tuple{values, &this->GetOutputSchema()};
  returned_ = true;
  return true;
}

}  // namespace bustub
