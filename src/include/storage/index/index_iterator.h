//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "common/logger.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT

  void Init(BufferPoolManager *init_buffer, page_id_t init_page_id, int init_offset);

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return it_page_id_ == itr.it_page_id_ && offset_ == itr.offset_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return it_page_id_ != itr.it_page_id_ || offset_ != itr.offset_;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  page_id_t it_page_id_;
  int offset_;
};

}  // namespace bustub
