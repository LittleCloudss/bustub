/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {
  it_page_id_ = INVALID_PAGE_ID;
  offset_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::Init(BufferPoolManager *init_buffer, page_id_t init_page_id, int init_offset) {
  buffer_pool_manager_ = init_buffer;
  it_page_id_ = init_page_id;
  offset_ = init_offset;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return it_page_id_ == INVALID_PAGE_ID && offset_ == 0; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto tmp = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(it_page_id_));
  buffer_pool_manager_->UnpinPage(it_page_id_, false);
  return tmp->PairAt(offset_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto tmp = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(it_page_id_));
  page_id_t next_page_id;
  if (offset_ < tmp->GetSize() - 1) {
    next_page_id = it_page_id_;
    offset_++;
  } else {
    next_page_id = tmp->GetNextPageId();
    offset_ = 0;
  }
  buffer_pool_manager_->UnpinPage(it_page_id_, false);
  it_page_id_ = next_page_id;
  // LOG_DEBUG("next_page_id %d %d", it_page_id_, offset_);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
