#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size, int pair_num)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      pair_num_(pair_num) {}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return pair_num_ == 0; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) -> LeafPage * {
  page_id_t now_page_id = root_page_id_;
  page_id_t pre_page_id;
  BPlusTreePage *tmp;
  while ((tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(now_page_id))) != nullptr) {
    if (tmp->IsLeafPage()) {
      auto leaf_page = reinterpret_cast<LeafPage *>(tmp);
      return leaf_page;
    }
    auto internal_page = reinterpret_cast<InternalPage *>(tmp);
    int size = internal_page->GetSize();
    int pos = 0;
    for (; pos < size - 1; pos++) {
      if (comparator_(internal_page->KeyAt(pos), key) == 1) {
        break;
      }
    }
    pre_page_id = now_page_id;
    now_page_id = internal_page->ValueAt(pos);
    buffer_pool_manager_->UnpinPage(pre_page_id, false);
  }
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInLeaf(LeafPage *leaf_page, const KeyType &key, std::vector<ValueType> *result) -> bool {
  int size = leaf_page->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (comparator_(leaf_page->KeyAt(pos), key) == 0) {
      result->push_back(leaf_page->ValueAt(pos));
      return true;
    }
    if (comparator_(leaf_page->KeyAt(pos), key) == 1) {
      break;
    }
  }
  return false;
}

// 0 duplicate  1 ok 2 maxsize
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(LeafPage *leaf_page, const KeyType &key, const ValueType &value) -> int {
  int size = leaf_page->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (comparator_(leaf_page->KeyAt(pos), key) == 0) {
      return 0;
    }
    if (comparator_(leaf_page->KeyAt(pos), key) == 1) {
      break;
    }
  }
  leaf_page->IncreaseSize(1);
  size = leaf_page->GetSize();
  for (int i = size - 1; i > pos; i--) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
  }
  leaf_page->SetKeyAt(pos, key);
  leaf_page->SetValueAt(pos, value);

  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    return 2;
  }
  return 1;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInInter(page_id_t parent_id, const KeyType &key, page_id_t L_id, page_id_t R_id) {
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id));
  if (parent->GetSize() == parent->GetMaxSize()) {
    SplitInInter(parent, key, L_id, R_id);
  } else {
    int size = parent->GetSize();
    int pos = 0;
    for (; pos < size - 1; pos++) {
      if (comparator_(parent->KeyAt(pos), key) == 1) {
        break;
      }
    }
    parent->IncreaseSize(1);
    size = parent->GetSize();
    for (int i = size - 1; i > pos; i--) {
      parent->SetKeyAt(i, parent->KeyAt(i - 1));
      parent->SetValueAt(i, parent->ValueAt(i - 1));
    }
    parent->SetKeyAt(pos, key);
    parent->SetValueAt(pos, L_id);
    parent->SetValueAt(pos + 1, R_id);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateNextPage(page_id_t parent_id, const KeyType &key, page_id_t l_id) {
  if (parent_id == INVALID_PAGE_ID) {
    return;
  }
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id));
  int size = parent->GetSize();
  int pos = 0;
  for (; pos < size - 1; pos++) {
    if (comparator_(parent->KeyAt(pos), key) == 1) {
      break;
    }
  }
  if (pos != 0) {
    auto left_leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(pos - 1)));
    left_leaf->SetNextPageId(l_id);
    buffer_pool_manager_->UnpinPage(left_leaf->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInLeaf(LeafPage *leaf_page) {
  page_id_t left_leaf_id;
  page_id_t right_leaf_id;
  auto left_leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&left_leaf_id));
  auto right_leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&right_leaf_id));
  assert(left_leaf != nullptr);
  assert(right_leaf != nullptr);

  left_leaf->Init(left_leaf_id);
  left_leaf->SetPageType(IndexPageType::LEAF_PAGE);
  left_leaf->SetMaxSize(leaf_max_size_);

  right_leaf->Init(right_leaf_id);
  right_leaf->SetPageType(IndexPageType::LEAF_PAGE);
  right_leaf->SetMaxSize(leaf_max_size_);

  left_leaf->SetNextPageId(right_leaf_id);
  right_leaf->SetNextPageId(leaf_page->GetNextPageId());
  UpdateNextPage(leaf_page->GetParentPageId(), leaf_page->KeyAt(0), left_leaf_id);

  left_leaf->SetSize(leaf_max_size_ / 2);
  right_leaf->SetSize(leaf_max_size_ - (leaf_max_size_ / 2));
  for (int i = 0; i < leaf_max_size_ / 2; i++) {
    left_leaf->SetKeyAt(i, leaf_page->KeyAt(i));
    left_leaf->SetValueAt(i, leaf_page->ValueAt(i));
  }

  for (int i = 0; i < leaf_max_size_ - (leaf_max_size_ / 2); i++) {
    right_leaf->SetKeyAt(i, leaf_page->KeyAt((leaf_max_size_ / 2) + i));
    right_leaf->SetValueAt(i, leaf_page->ValueAt((leaf_max_size_ / 2) + i));
  }

  if (leaf_page->IsRootPage()) {
    auto root = reinterpret_cast<InternalPage *>(leaf_page);
    root->SetSize(2);
    root->SetPageType(IndexPageType::ROOT_INTERNAL_PAGE);
    root->SetMaxSize(internal_max_size_);
    root->SetKeyAt(0, right_leaf->KeyAt(0));
    root->SetKeyAt(1, right_leaf->KeyAt(0));
    root->SetValueAt(0, left_leaf->GetPageId());
    root->SetValueAt(1, right_leaf->GetPageId());
    left_leaf->SetParentPageId(root->GetPageId());
    right_leaf->SetParentPageId(root->GetPageId());
    buffer_pool_manager_->UnpinPage(left_leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
  } else {
    left_leaf->SetParentPageId(leaf_page->GetParentPageId());
    right_leaf->SetParentPageId(leaf_page->GetParentPageId());
    InsertInInter(leaf_page->GetParentPageId(), right_leaf->KeyAt(0), left_leaf_id, right_leaf_id);

    buffer_pool_manager_->UnpinPage(left_leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInInter(InternalPage *parent, const KeyType &key, page_id_t L_id, page_id_t R_id) {
  page_id_t left_inter_id;
  page_id_t right_inter_id;
  auto left_inter = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&left_inter_id));
  auto right_inter = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&right_inter_id));
  left_inter->Init(left_inter_id);
  left_inter->SetPageType(IndexPageType::INTERNAL_PAGE);
  left_inter->SetMaxSize(internal_max_size_);
  right_inter->Init(right_inter_id);
  right_inter->SetPageType(IndexPageType::INTERNAL_PAGE);
  right_inter->SetMaxSize(internal_max_size_);

  left_inter->SetSize((internal_max_size_ + 1) / 2);
  right_inter->SetSize((internal_max_size_ + 1) - ((internal_max_size_ + 1) / 2));
  int add_new = 0;
  int r_flag = 0;

  for (int i = 0; i < left_inter->GetSize(); i++) {
    if (add_new == 0 && comparator_(key, parent->KeyAt(i)) == -1) {
      add_new = 1;
      left_inter->SetKeyAt(i, key);
      left_inter->SetValueAt(i, L_id);
      r_flag = 1;
    } else {
      left_inter->SetKeyAt(i, parent->KeyAt(i - add_new));
      left_inter->SetValueAt(i, parent->ValueAt(i - add_new));
      if (r_flag != 0) {
        left_inter->SetValueAt(i, R_id);
        r_flag = 0;
      }
    }
    auto temp = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left_inter->ValueAt(i)));
    temp->SetParentPageId(left_inter_id);
    buffer_pool_manager_->UnpinPage(left_inter->ValueAt(i), true);
  }

  for (int i = 0; i < right_inter->GetSize(); i++) {
    if (add_new == 0 &&
        (comparator_(key, parent->KeyAt(left_inter->GetSize() + i)) == -1 || i == right_inter->GetSize() - 2)) {
      add_new = 1;
      right_inter->SetKeyAt(i, key);
      right_inter->SetValueAt(i, L_id);
      r_flag = 1;
    } else {
      right_inter->SetKeyAt(i, parent->KeyAt(left_inter->GetSize() + i - add_new));
      right_inter->SetValueAt(i, parent->ValueAt(left_inter->GetSize() + i - add_new));
      if (r_flag != 0) {
        right_inter->SetValueAt(i, R_id);
        r_flag = 0;
      }
    }
    auto temp = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(right_inter->ValueAt(i)));
    temp->SetParentPageId(right_inter_id);
    buffer_pool_manager_->UnpinPage(right_inter->ValueAt(i), true);
  }

  if (parent->IsRootPage()) {
    parent->SetSize(2);
    parent->SetPageType(IndexPageType::ROOT_INTERNAL_PAGE);
    parent->SetKeyAt(0, parent->KeyAt(left_inter->GetSize() - 1));
    parent->SetKeyAt(1, parent->KeyAt(left_inter->GetSize() - 1));
    parent->SetValueAt(0, left_inter->GetPageId());
    parent->SetValueAt(1, right_inter->GetPageId());
    left_inter->SetParentPageId(parent->GetPageId());
    right_inter->SetParentPageId(parent->GetPageId());
    buffer_pool_manager_->UnpinPage(left_inter->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_inter->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    left_inter->SetParentPageId(parent->GetParentPageId());
    right_inter->SetParentPageId(parent->GetParentPageId());
    InsertInInter(parent->GetParentPageId(), parent->KeyAt(left_inter->GetSize() - 1), left_inter_id, right_inter_id);
    buffer_pool_manager_->UnpinPage(left_inter->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_inter->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->DeletePage(parent->GetPageId());
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteInLeaf(LeafPage *leaf_page, const KeyType &key) -> bool {
  int size = leaf_page->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (comparator_(leaf_page->KeyAt(pos), key) == 0) {
      break;
    }
  }
  if (comparator_(leaf_page->KeyAt(pos), key) != 0) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  leaf_page->IncreaseSize(-1);
  size = leaf_page->GetSize();
  for (int i = pos; i < size; i++) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
  }
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    Merge(reinterpret_cast<BPlusTreePage *>(leaf_page));
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *now_page) {
  if (now_page->IsRootPage()) {
    return;
  }
  if (!BorrowFromLeft(now_page) && !BorrowFromRight(now_page)) {
    if (MergeToLeft(now_page)) {
      return;
    }
    assert(MergeToRight(now_page));
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromLeft(BPlusTreePage *now_page) -> bool {
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(now_page->GetParentPageId()));
  int size = parent->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (parent->ValueAt(pos) == now_page->GetPageId()) {
      break;
    }
  }
  if (pos == 0) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    return false;
  }
  auto left = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(pos - 1)));
  if (left->GetSize() <= left->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(left->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    return false;
  }
  if (now_page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(now_page);
    auto left_leaf_page = reinterpret_cast<LeafPage *>(left);
    leaf_page->IncreaseSize(1);
    for (int i = leaf_page->GetSize(); i > 0; i--) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
    }
    leaf_page->SetKeyAt(0, left_leaf_page->KeyAt(left_leaf_page->GetSize() - 1));
    leaf_page->SetValueAt(0, left_leaf_page->ValueAt(left_leaf_page->GetSize() - 1));
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(now_page);
    auto left_internal_page = reinterpret_cast<InternalPage *>(left);
    internal_page->IncreaseSize(1);
    for (int i = internal_page->GetSize(); i > 0; i--) {
      internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));
      internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));
    }
    internal_page->SetKeyAt(0, left_internal_page->KeyAt(left_internal_page->GetSize() - 1));
    internal_page->SetValueAt(0, left_internal_page->ValueAt(left_internal_page->GetSize() - 1));
  }
  left->IncreaseSize(-1);
  buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromRight(BPlusTreePage *now_page) -> bool {
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(now_page->GetParentPageId()));
  int size = parent->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (parent->ValueAt(pos) == now_page->GetPageId()) {
      break;
    }
  }
  if (pos >= size - 1) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    return false;
  }
  auto right = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(pos + 1)));
  if (right->GetSize() <= right->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(right->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    return false;
  }
  if (now_page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(now_page);
    auto right_leaf_page = reinterpret_cast<LeafPage *>(right);
    leaf_page->IncreaseSize(1);
    leaf_page->SetKeyAt(leaf_page->GetSize() - 1, right_leaf_page->KeyAt(0));
    leaf_page->SetValueAt(leaf_page->GetSize() - 1, right_leaf_page->ValueAt(0));
    for (int i = 0; i < right_leaf_page->GetSize() - 1; i++) {
      right_leaf_page->SetKeyAt(i, right_leaf_page->KeyAt(i + 1));
      right_leaf_page->SetValueAt(i, right_leaf_page->ValueAt(i + 1));
    }
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(now_page);
    auto right_internal_page = reinterpret_cast<InternalPage *>(right);
    internal_page->IncreaseSize(1);
    internal_page->SetKeyAt(internal_page->GetSize() - 1, right_internal_page->KeyAt(0));
    internal_page->SetValueAt(internal_page->GetSize() - 1, right_internal_page->ValueAt(0));
    for (int i = 0; i < right_internal_page->GetSize() - 1; i++) {
      right_internal_page->SetKeyAt(i, right_internal_page->KeyAt(i + 1));
      right_internal_page->SetValueAt(i, right_internal_page->ValueAt(i + 1));
    }
  }
  right->IncreaseSize(-1);
  buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetKeyFromParent(InternalPage *parent, const KeyType &key) -> KeyType {
  int size = parent->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (comparator_(key, parent->KeyAt(pos)) == -1) {
      return parent->KeyAt(pos);
    }
  }
  assert(0);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteInInter(InternalPage *parent, const KeyType &key) {
  int size = parent->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (comparator_(key, parent->KeyAt(pos)) == -1) {
      break;
    }
  }
  parent->IncreaseSize(-1);
  size = parent->GetSize();
  for (int i = pos + 1; i < size; i++) {
    parent->SetKeyAt(i, parent->KeyAt(i + 1));
    parent->SetValueAt(i, parent->ValueAt(i + 1));
  }
  if (parent->IsRootPage()) {
    if (size == 1) {
      root_page_id_ = parent->ValueAt(0);
      UpdateRootPageId();
      auto new_root = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
      if (new_root->IsLeafPage()) {
        new_root->SetPageType(IndexPageType::ROOT_LEAF_PAGE);
      } else {
        new_root->SetPageType(IndexPageType::ROOT_INTERNAL_PAGE);
      }
      new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
    return;
  }
  if (parent->GetSize() < parent->GetMinSize()) {
    Merge(reinterpret_cast<BPlusTreePage *>(parent));
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeToLeft(BPlusTreePage *now_page) -> bool {
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(now_page->GetParentPageId()));
  int size = parent->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (parent->ValueAt(pos) == now_page->GetPageId()) {
      break;
    }
  }
  if (pos == 0) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    return false;
  }
  auto left = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(pos - 1)));
  assert(left->GetSize() <= left->GetMinSize());
  if (now_page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(now_page);
    auto left_leaf_page = reinterpret_cast<LeafPage *>(left);
    for (int i = 0; i < leaf_page->GetSize(); i++) {
      left_leaf_page->SetKeyAt(i + left_leaf_page->GetSize(), leaf_page->KeyAt(i));
      left_leaf_page->SetValueAt(i + left_leaf_page->GetSize(), leaf_page->ValueAt(i));
    }
    left_leaf_page->IncreaseSize(leaf_page->GetSize());
    left_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    DeleteInInter(parent, left_leaf_page->KeyAt(0));
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(now_page);
    auto left_internal_page = reinterpret_cast<InternalPage *>(left);
    for (int i = 0; i < internal_page->GetSize(); i++) {
      left_internal_page->SetKeyAt(i + left_internal_page->GetSize(), internal_page->KeyAt(i));
      left_internal_page->SetValueAt(i + left_internal_page->GetSize(), internal_page->ValueAt(i));
    }
    KeyType mid_key = GetKeyFromParent(parent, left_internal_page->KeyAt(0));
    left_internal_page->SetKeyAt(left_internal_page->GetSize() - 1, mid_key);
    left_internal_page->IncreaseSize(internal_page->GetSize());
    DeleteInInter(parent, left_internal_page->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeToRight(BPlusTreePage *now_page) -> bool {
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(now_page->GetParentPageId()));
  int size = parent->GetSize();
  int pos = 0;
  for (; pos < size; pos++) {
    if (parent->ValueAt(pos) == now_page->GetPageId()) {
      break;
    }
  }
  if (pos >= size - 1) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    return false;
  }
  auto right = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(pos + 1)));
  assert(right->GetSize() <= right->GetMinSize());
  if (now_page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(now_page);
    auto right_leaf_page = reinterpret_cast<LeafPage *>(right);
    for (int i = 0; i < right_leaf_page->GetSize(); i++) {
      leaf_page->SetKeyAt(i + leaf_page->GetSize(), right_leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i + leaf_page->GetSize(), right_leaf_page->ValueAt(i));
    }
    leaf_page->IncreaseSize(right_leaf_page->GetSize());
    leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
    DeleteInInter(parent, leaf_page->KeyAt(0));
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(now_page);
    auto right_internal_page = reinterpret_cast<InternalPage *>(right);
    for (int i = 0; i < right_internal_page->GetSize(); i++) {
      internal_page->SetKeyAt(i + internal_page->GetSize(), right_internal_page->KeyAt(i));
      internal_page->SetValueAt(i + internal_page->GetSize(), right_internal_page->ValueAt(i));
    }
    KeyType mid_key = GetKeyFromParent(parent, internal_page->KeyAt(0));
    internal_page->SetKeyAt(internal_page->GetSize() - 1, mid_key);
    internal_page->IncreaseSize(right_internal_page->GetSize());
    DeleteInInter(parent, internal_page->KeyAt(0));
  }
  right->IncreaseSize(-1);
  buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CheckPageClear() {
  int num = buffer_pool_manager_->GetPoolSize();
  page_id_t page[num];
  for (int i = 0; i < num; i++) {
    if (buffer_pool_manager_->NewPage(&page[i]) == nullptr) {
      num = i;
      printf("res = %d\n", num);
      break;
    }
  }
  for (int i = 0; i < num; i++) {
    buffer_pool_manager_->UnpinPage(page[i], false);
    buffer_pool_manager_->DeletePage(page[i]);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  auto leaf_page = GetLeafPage(key);
  bool ret = FindInLeaf(leaf_page, key, result);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    auto tmp = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_));
    UpdateRootPageId();
    assert(tmp != nullptr);
    tmp->Init(root_page_id_);
    tmp->SetPageType(IndexPageType::ROOT_LEAF_PAGE);
    tmp->IncreaseSize(1);
    tmp->SetKeyAt(0, key);
    tmp->SetValueAt(0, value);
    tmp->SetMaxSize(leaf_max_size_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    pair_num_++;
    return true;
  }

  LeafPage *leaf_page = GetLeafPage(key);

  int ret = InsertInLeaf(leaf_page, key, value);
  if (ret != 0) {
    pair_num_++;
  }
  if (ret != 2) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return ret != 0;
  }
  SplitInLeaf(leaf_page);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  LeafPage *leaf_page = GetLeafPage(key);
  int ret = DeleteInLeaf(leaf_page, key);
  if (ret != 0) {
    pair_num_--;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  // CheckPageClear();
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
