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
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuple_.emplace_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuple_.emplace_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Match(Tuple *left, Tuple *right) -> bool {
  auto value = plan_->Predicate().EvaluateJoin(left, left_executor_->GetOutputSchema(), right,
                                               right_executor_->GetOutputSchema());
  return (!value.IsNull() && value.GetAs<bool>());
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_pos_ < left_tuple_.size()) {
    if (right_pos_ == right_tuple_.size()) {
      if (!matched_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
          value.emplace_back(left_tuple_[left_pos_].GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
          value.emplace_back(
              ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple{value, &this->GetOutputSchema()};
        matched_ = true;
        return true;
      }
      right_pos_ = 0;
      matched_ = false;
      left_pos_++;
      if (left_pos_ == left_tuple_.size()) {
        return false;
      }
    }
    if (right_tuple_.empty()) {
      continue;
    }
    if (Match(&left_tuple_[left_pos_], &right_tuple_[right_pos_])) {
      std::vector<Value> value;
      for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
        value.emplace_back(left_tuple_[left_pos_].GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
        value.emplace_back(right_tuple_[right_pos_].GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
      }
      *tuple = Tuple{value, &this->GetOutputSchema()};
      matched_ = true;
      right_pos_++;
      return true;
    }
    right_pos_++;
  }
  return false;
}

}  // namespace bustub
