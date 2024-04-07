#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

auto SortExecutor::Cmp(const Tuple *tuple1, const Tuple *tuple2) -> bool {
  for (auto const &order : this->plan_->GetOrderBy()) {
    CmpBool ret = (order.second->Evaluate(tuple1, child_executor_->GetOutputSchema())
                       .CompareEquals(order.second->Evaluate(tuple2, child_executor_->GetOutputSchema())));
    if (ret == CmpBool::CmpTrue) {
      continue;
    }
    if (order.first == OrderByType::ASC || order.first == OrderByType::DEFAULT) {
      ret = (order.second->Evaluate(tuple1, child_executor_->GetOutputSchema())
                 .CompareLessThan(order.second->Evaluate(tuple2, child_executor_->GetOutputSchema())));
      return ret == CmpBool::CmpTrue;
    }
    if (order.first == OrderByType::DESC) {
      ret = (order.second->Evaluate(tuple1, child_executor_->GetOutputSchema())
                 .CompareGreaterThan(order.second->Evaluate(tuple2, child_executor_->GetOutputSchema())));
      return ret == CmpBool::CmpTrue;
    }
  }
  return true;
}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    result_.emplace_back(tuple);
  }
  std::sort(result_.begin(), result_.end(), [this](const Tuple &a, const Tuple &b) { return Cmp(&a, &b); });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (pos_ == result_.size()) {
    return false;
  }
  *tuple = result_[pos_];
  *rid = tuple->GetRid();
  pos_++;
  return true;
}

}  // namespace bustub
