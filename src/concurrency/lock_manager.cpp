//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::MaintainTableSetState(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool is_insert)
    -> void {
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (is_insert) {
      txn->GetExclusiveTableLockSet()->insert(oid);
    } else {
      txn->GetExclusiveTableLockSet()->erase(oid);
    }
  }

  if (lock_mode == LockMode::SHARED) {
    if (is_insert) {
      txn->GetSharedTableLockSet()->insert(oid);
    } else {
      txn->GetSharedTableLockSet()->erase(oid);
    }
  }

  if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (is_insert) {
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    } else {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    }
  }

  if (lock_mode == LockMode::INTENTION_SHARED) {
    if (is_insert) {
      txn->GetIntentionSharedTableLockSet()->insert(oid);
    } else {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    }
  }

  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (is_insert) {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
    } else {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    }
  }
}

auto LockManager::MaintainRowSetState(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                                      bool is_insert) -> void {
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> set;
  if (lock_mode == LockMode::EXCLUSIVE) {
    set = txn->GetExclusiveRowLockSet();
  }
  if (lock_mode == LockMode::SHARED) {
    set = txn->GetSharedRowLockSet();
  }
  if (is_insert) {
    if (set->find(oid) == set->end()) {
      set->emplace(oid, std::unordered_set<RID>{});
    }
    set->find(oid)->second.insert(rid);
  } else {
    set->find(oid)->second.erase(rid);
  }
}

auto LockManager::GrantLock(const std::list<std::shared_ptr<LockRequest>> &request_queue_,
                            const std::shared_ptr<LockRequest> &new_request) -> bool {
  for (auto request : request_queue_) {  // NOLINT
    if (request->granted_) {
      if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
        if (new_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
        if (new_request->lock_mode_ == LockMode::SHARED ||
            new_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            new_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::SHARED) {
        if (new_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
            new_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            new_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        if (new_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || new_request->lock_mode_ == LockMode::SHARED ||
            new_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            new_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    } else if (request == new_request) {
      return true;
    } else {
      return false;
    }
  }
  return false;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
  //   assert(0);
  // }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    } else {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto now = table_lock_map_[oid];
  now->latch_.lock();
  table_lock_map_latch_.unlock();
  for (auto request : now->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        now->latch_.unlock();
        return true;
      }
      if (now->upgrading_ != INVALID_TXN_ID) {
        now->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
        if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::SHARED) {
        if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::EXCLUSIVE) {
        now->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }

      now->request_queue_.remove(request);
      MaintainTableSetState(txn, request->lock_mode_, request->oid_, false);

      auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      auto itr = now->request_queue_.begin();
      while (itr != now->request_queue_.end()) {
        if (!(*itr)->granted_) {
          break;
        }
        itr++;
      }

      now->request_queue_.insert(itr, new_request);
      now->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(now->latch_, std::adopt_lock);
      while (!GrantLock(now->request_queue_, new_request)) {
        now->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          now->request_queue_.remove(new_request);
          now->upgrading_ = INVALID_TXN_ID;
          // now->latch_.unlock();
          now->cv_.notify_all();
          return false;
        }
      }
      MaintainTableSetState(txn, lock_mode, oid, true);
      now->upgrading_ = INVALID_TXN_ID;
      new_request->granted_ = true;
      // now->latch_.unlock();
      now->cv_.notify_all();
      return true;
    }
  }
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  now->request_queue_.push_back(new_request);

  std::unique_lock<std::mutex> lock(now->latch_, std::adopt_lock);
  while (!GrantLock(now->request_queue_, new_request)) {
    now->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      assert(!now->request_queue_.empty());
      now->request_queue_.remove(new_request);
      // now->latch_.unlock();
      now->cv_.notify_all();
      return false;
    }
  }

  new_request->granted_ = true;
  MaintainTableSetState(txn, lock_mode, oid, true);

  // now->latch_.unlock();
  now->cv_.notify_all();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
       !txn->GetSharedRowLockSet()->find(oid)->second.empty()) ||
      (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() &&
       !txn->GetExclusiveRowLockSet()->find(oid)->second.empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  auto now = table_lock_map_[oid];

  now->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto request : now->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      now->request_queue_.remove(request);
      MaintainTableSetState(txn, request->lock_mode_, request->oid_, false);
      now->latch_.unlock();
      now->cv_.notify_all();
      return true;
    }
  }

  now->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
  //   assert(0);
  // }
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    } else {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
  }

  if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto now = row_lock_map_[rid];
  now->latch_.lock();
  row_lock_map_latch_.unlock();
  for (auto request : now->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        now->latch_.unlock();
        return true;
      }
      if (now->upgrading_ != INVALID_TXN_ID) {
        now->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
        if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::SHARED) {
        if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        if (lock_mode != LockMode::EXCLUSIVE) {
          now->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          return false;
        }
      }
      now->request_queue_.remove(request);
      MaintainRowSetState(txn, request->lock_mode_, request->oid_, request->rid_, false);
      auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      auto itr = now->request_queue_.begin();
      while (itr != now->request_queue_.end()) {
        if (!(*itr)->granted_) {
          break;
        }
        itr++;
      }

      now->request_queue_.insert(itr, new_request);
      now->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(now->latch_, std::adopt_lock);
      while (!GrantLock(now->request_queue_, new_request)) {
        now->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          now->request_queue_.remove(new_request);
          now->upgrading_ = INVALID_TXN_ID;
          // now->latch_.unlock();
          now->cv_.notify_all();
          return false;
        }
      }

      now->upgrading_ = INVALID_TXN_ID;
      new_request->granted_ = true;
      MaintainRowSetState(txn, lock_mode, oid, rid, true);

      // now->latch_.unlock();
      now->cv_.notify_all();
      return true;
    }
  }
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

  now->request_queue_.push_back(new_request);

  std::unique_lock<std::mutex> lock(now->latch_, std::adopt_lock);
  while (!GrantLock(now->request_queue_, new_request)) {
    now->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      assert(!now->request_queue_.empty());
      now->request_queue_.remove(new_request);
      // now->latch_.unlock();
      now->cv_.notify_all();
      return false;
    }
  }

  new_request->granted_ = true;
  MaintainRowSetState(txn, lock_mode, oid, rid, true);

  // now->latch_.unlock();
  now->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  auto now = row_lock_map_[rid];

  now->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto request : now->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      now->request_queue_.remove(request);
      MaintainRowSetState(txn, request->lock_mode_, request->oid_, request->rid_, false);
      now->latch_.unlock();
      now->cv_.notify_all();
      return true;
    }
  }

  now->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.emplace(t1, std::vector<txn_id_t>{});
  }
  for (auto tid : waits_for_.find(t1)->second) {
    if (tid == t2) {
      return;
    }
  }
  vertex_.emplace_back(t1);
  waits_for_.find(t1)->second.emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    return;
  }
  for (auto itr = waits_for_.find(t1)->second.begin(); itr != waits_for_.find(t1)->second.end(); itr++) {
    if (*itr == t2) {
      waits_for_.find(t1)->second.erase(itr);
      return;
    }
  }
}

auto LockManager::Dfs(txn_id_t now, txn_id_t *max_tid) -> bool {
  tags_[now] = true;
  if (waits_for_.find(now) == waits_for_.end()) {
    tags_.erase(now);
    return false;
  }
  std::vector<txn_id_t> edge = waits_for_.find(now)->second;
  if (edge.empty()) {
    tags_.erase(now);
    return false;
  }
  sort(edge.begin(), edge.end());
  auto predicate = [&](auto tid) { return (tags_.find(tid) != tags_.end()) || Dfs(tid, max_tid); };
  bool found = std::any_of(edge.begin(), edge.end(), predicate);
  if (found) {
    if (now > *max_tid) {
      *max_tid = now;
    }
    return true;
  }
  tags_.erase(now);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  if (waits_for_.empty()) {
    return false;
  }
  sort(vertex_.begin(), vertex_.end());
  for (auto tid : vertex_) {
    txn_id_t max_tid = 0;
    tags_.clear();
    if (Dfs(tid, &max_tid)) {
      *txn_id = max_tid;
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto const &vec : waits_for_) {
    for (auto tid : vec.second) {
      edges.emplace_back(std::make_pair(vec.first, tid));
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    waits_for_latch_.lock();
    std::unordered_map<txn_id_t, std::vector<table_oid_t>> want_table;
    std::unordered_map<txn_id_t, std::vector<RID>> want_row;
    table_lock_map_latch_.lock();
    for (auto &table : table_lock_map_) {
      table.second->latch_.lock();
      std::vector<txn_id_t> granted;
      granted.clear();
      for (auto const &request : table.second->request_queue_) {
        if (request->granted_) {
          granted.emplace_back(request->txn_id_);
        } else {
          for (auto tid : granted) {
            AddEdge(request->txn_id_, tid);
            if (want_table.find(request->txn_id_) == want_table.end()) {
              want_table.emplace(request->txn_id_, std::vector<table_oid_t>{});
            }
            want_table.find(request->txn_id_)->second.emplace_back(table.first);
          }
        }
      }
      table.second->latch_.unlock();
    }
    table_lock_map_latch_.unlock();
    row_lock_map_latch_.lock();
    for (auto &row : row_lock_map_) {
      row.second->latch_.lock();
      std::vector<txn_id_t> granted;
      granted.clear();
      for (auto const &request : row.second->request_queue_) {
        if (request->granted_) {
          granted.emplace_back(request->txn_id_);
        } else {
          for (auto tid : granted) {
            AddEdge(request->txn_id_, tid);
            if (want_row.find(request->txn_id_) == want_row.end()) {
              want_row.emplace(request->txn_id_, std::vector<RID>{});
            }
            want_row.find(request->txn_id_)->second.emplace_back(row.first);
          }
        }
      }
      row.second->latch_.unlock();
    }
    row_lock_map_latch_.unlock();

    std::vector<txn_id_t> deleted;
    txn_id_t txn_id;
    while (HasCycle(&txn_id)) {
      // for (auto tmp : GetEdgeList()) {
      //   LOG_DEBUG("%d %d", tmp.first, tmp.second);
      // }
      // assert(0);
      std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
      for (auto const &vec : waits_for_) {
        for (auto const &tid : vec.second) {
          if (vec.first == txn_id || tid == txn_id) {
            edges.emplace_back(std::make_pair(vec.first, tid));
          }
        }
      }
      for (auto edge : edges) {
        RemoveEdge(edge.first, edge.second);
      }
      TransactionManager::GetTransaction(txn_id)->SetState(TransactionState::ABORTED);
      deleted.emplace_back(txn_id);
    }

    for (auto tid : deleted) {
      if (want_table.find(tid) != want_table.end()) {
        table_lock_map_latch_.lock();
        for (auto table : want_table.find(tid)->second) {
          table_lock_map_.find(table)->second->latch_.lock();
          table_lock_map_.find(table)->second->cv_.notify_all();
          table_lock_map_.find(table)->second->latch_.unlock();
        }
        table_lock_map_latch_.unlock();
      }
      if (want_row.find(tid) != want_row.end()) {
        row_lock_map_latch_.lock();
        for (auto row : want_row.find(tid)->second) {
          row_lock_map_.find(row)->second->latch_.lock();
          row_lock_map_.find(row)->second->cv_.notify_all();
          row_lock_map_.find(row)->second->latch_.unlock();
        }
        row_lock_map_latch_.unlock();
      }
    }
    waits_for_.clear();
    want_table.clear();
    want_row.clear();
    deleted.clear();
    vertex_.clear();
    tags_.clear();
    waits_for_latch_.unlock();
  }
}

}  // namespace bustub
