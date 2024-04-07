//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          this->exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Match(Tuple *left, Tuple *right) -> bool {
  auto value =
      plan_->KeyPredicate()->EvaluateJoin(left, child_executor_->GetOutputSchema(), right, plan_->InnerTableSchema());
  return (!value.IsNull() && value.GetAs<bool>());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    std::vector<RID> result;
    tree_->ScanKey(
        Tuple{{value}, this->exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_->GetKeySchema()}, &result,
        this->exec_ctx_->GetTransaction());
    if (!result.empty()) {
      this->exec_ctx_->GetCatalog()
          ->GetTable(this->exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->table_name_)
          ->table_->GetTuple(result[0], &right_tuple, this->exec_ctx_->GetTransaction());
      std::vector<Value> value;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        value.emplace_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        value.emplace_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
      }
      *tuple = Tuple{value, &this->GetOutputSchema()};
      return true;
    }
    if (this->plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> value;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        value.emplace_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        value.emplace_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{value, &this->GetOutputSchema()};
      return true;
    }
  }
  return false;
}

}  // namespace bustub
