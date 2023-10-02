//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : record_pair_(new std::pair<frame_id_t, std::deque<TimePoint>>[num_frames]),
      evictable_tag_(new bool[num_frames]),
      replacer_size_(num_frames),
      k_(k) {
  for (size_t i = 0; i < num_frames; i++) {
    evictable_tag_[i] = false;
    record_pair_[i].first = -1;
    record_pair_[i].second.clear();
  }
}

LRUKReplacer::~LRUKReplacer() {
  delete[] record_pair_;
  delete[] evictable_tag_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  bool flag = false;
  int index = 0;
  std::chrono::nanoseconds max_duration = std::chrono::nanoseconds::min();
  TimePoint min_inf_timestamp = TimePoint::max();
  for (size_t i = 0; i < replacer_size_; i++) {
    if (evictable_tag_[i]) {
      if (record_pair_[i].second.size() < k_) {
        if (record_pair_[i].second.front() < min_inf_timestamp) {
          min_inf_timestamp = record_pair_[i].second.front();
          *frame_id = record_pair_[i].first;
          index = i;
        }
      } else {
        if (min_inf_timestamp == TimePoint::max()) {
          if (max_duration < record_pair_[i].second.back() - record_pair_[i].second.front()) {
            max_duration = record_pair_[i].second.back() - record_pair_[i].second.front();
            *frame_id = record_pair_[i].first;
            index = i;
          }
        }
      }
      flag = true;
    }
  }
  if (flag) {
    evictable_tag_[index] = false;
    record_pair_[index].first = -1;
    record_pair_[index].second.clear();
    curr_size_--;
  }
  latch_.unlock();
  return flag;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  bool flag = false;
  TimePoint current_time = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < replacer_size_; i++) {
    if (record_pair_[i].first == frame_id) {
      if (record_pair_[i].second.size() < k_) {
        record_pair_[i].second.push_back(current_time);
      } else {
        record_pair_[i].second.pop_front();
        record_pair_[i].second.push_back(current_time);
      }
      flag = true;
    }
  }
  if (!flag) {
    for (size_t i = 0; i < replacer_size_; i++) {
      if (record_pair_[i].first == -1) {
        record_pair_[i].first = frame_id;
        record_pair_[i].second.push_back(current_time);
        flag = true;
        break;
      }
    }
  }
  BUSTUB_ASSERT(flag, "frame id is invalid");
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  // LOG_DEBUG("before set curr_size_ is %d", (int)curr_size_);
  bool flag = false;
  for (size_t i = 0; i < replacer_size_; i++) {
    if (record_pair_[i].first == frame_id) {
      if (evictable_tag_[i] && !set_evictable) {
        curr_size_--;
      } else if (!evictable_tag_[i] && set_evictable) {
        curr_size_++;
      }
      evictable_tag_[i] = set_evictable;
      flag = true;
    }
  }
  // LOG_DEBUG("curr_size_ is %d", (int)curr_size_);
  BUSTUB_ASSERT(flag, "frame id is invalid");
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  for (size_t i = 0; i < replacer_size_; i++) {
    if (record_pair_[i].first == frame_id) {
      BUSTUB_ASSERT(evictable_tag_[i], "frame can't be removed");
      evictable_tag_[i] = false;
      record_pair_[i].first = -1;
      record_pair_[i].second.clear();
      curr_size_--;
    }
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  size_t ret = curr_size_;
  latch_.unlock();
  return ret;
}

}  // namespace bustub
