#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

auto TopNExecutor::Cmp(const Tuple *tuple1, const Tuple *tuple2) -> bool {
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

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp_lamda = [this](const Tuple &a, const Tuple &b) { return Cmp(&a, &b); };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp_lamda)> q(cmp_lamda);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (q.size() < this->plan_->GetN()) {
      q.push(tuple);
    } else {
      if (!Cmp(&q.top(), &tuple)) {
        q.pop();
        q.push(tuple);
      }
    }
  }
  std::vector<Tuple> tmp;
  while (!q.empty()) {
    tmp.emplace_back(q.top());
    q.pop();
  }
  for (int i = tmp.size() - 1; i >= 0; i--) {
    result_.emplace_back(tmp[i]);
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (pos_ == result_.size()) {
    return false;
  }
  *tuple = result_[pos_];
  *rid = tuple->GetRid();
  pos_++;
  return true;
}

}  // namespace bustub
