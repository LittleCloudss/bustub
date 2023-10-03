//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  if (free_list_.size() != 0) {
    frame_id_t free_frame = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, free_frame);
    pages_[free_frame].is_dirty_ = false;
    pages_[free_frame].page_id_ = *page_id;
    pages_[free_frame].pin_count_ = 1;
    pages_[free_frame].ResetMemory();
    replacer_->RecordAccess(free_frame);
    replacer_->SetEvictable(free_frame, false);
    return &pages_[free_frame];
  }
  frame_id_t ret;
  if (!replacer_->Evict(&ret)) return nullptr;
  page_table_->Remove(pages_[ret].GetPageId());
  if (pages_[ret].IsDirty()) {
    disk_manager_->WritePage(pages_[ret].GetPageId(), pages_[ret].GetData());
  }
  *page_id = AllocatePage();
  page_table_->Insert(*page_id, ret);
  pages_[ret].is_dirty_ = false;
  pages_[ret].page_id_ = *page_id;
  pages_[ret].pin_count_ = 1;
  pages_[ret].ResetMemory();
  replacer_->RecordAccess(ret);
  replacer_->SetEvictable(ret, false);
  return &pages_[ret];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t buffer_ret;
  if (page_table_->Find(page_id, buffer_ret)) {
    return &pages_[buffer_ret];
  }
  if (free_list_.size() != 0) {
    frame_id_t free_frame = free_list_.front();
    free_list_.pop_front();
    page_table_->Insert(page_id, free_frame);
    pages_[free_frame].is_dirty_ = false;
    pages_[free_frame].page_id_ = page_id;
    pages_[free_frame].pin_count_ = 1;
    disk_manager_->ReadPage(page_id, pages_[free_frame].GetData());
    replacer_->RecordAccess(free_frame);
    replacer_->SetEvictable(free_frame, false);
    return &pages_[free_frame];
  }
  frame_id_t ret;
  if (!replacer_->Evict(&ret)) return nullptr;
  page_table_->Remove(pages_[ret].GetPageId());
  if (pages_[ret].IsDirty()) {
    disk_manager_->WritePage(pages_[ret].GetPageId(), (char *)&pages_[ret]);
  }
  page_table_->Insert(page_id, ret);
  pages_[ret].is_dirty_ = false;
  pages_[ret].page_id_ = page_id;
  pages_[ret].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, (char *)&pages_[ret]);
  replacer_->RecordAccess(ret);
  replacer_->SetEvictable(ret, false);
  return &pages_[ret];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t buffer_ret;
  if (page_table_->Find(page_id, buffer_ret)) {
    if (pages_[buffer_ret].GetPinCount() == 0) {
      return false;
    }
    pages_[buffer_ret].pin_count_--;
    if (pages_[buffer_ret].pin_count_ == 0) replacer_->SetEvictable(buffer_ret, true);
    pages_[buffer_ret].is_dirty_ = is_dirty;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t buffer_ret;
  if (page_table_->Find(page_id, buffer_ret)) {
    disk_manager_->WritePage(pages_[buffer_ret].GetPageId(), pages_[buffer_ret].GetData());
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  bool unused[pool_size_];
  memset(unused, 0, sizeof(bool) * pool_size_);
  for (auto it = free_list_.begin(); it != free_list_.end(); ++it) {
    unused[*it] = true;
  }
  for (size_t i = 0; i < pool_size_; i++)
    if (!unused[i]) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t buffer_ret;
  if (page_table_->Find(page_id, buffer_ret)) {
    if (pages_[buffer_ret].pin_count_ != 0) {
      return false;
    }
    replacer_->Remove(buffer_ret);
    page_table_->Remove(page_id);
    free_list_.push_back(buffer_ret);
    pages_[buffer_ret].is_dirty_ = false;
    pages_[buffer_ret].page_id_ = INVALID_PAGE_ID;
    pages_[buffer_ret].pin_count_ = 0;
    pages_[buffer_ret].ResetMemory();
    DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
