/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai.wyl on 2021/5/19.
//

#include "storage/index/bplus_tree_index.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/db/db.h"

static int calc_composite_length(const std::vector<BplusTreeIndex::FieldHolder> &fields) {
  int total = 0; for (auto &f : fields) total += f.len; return total;
}
static void fill_composite_key(const std::vector<BplusTreeIndex::FieldHolder> &fields, const char *record, char *dest) {
  int offset = 0; for (auto &f : fields) { memcpy(dest + offset, record + f.offset, f.len); offset += f.len; }
}

BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

RC BplusTreeIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const std::vector<const FieldMeta *> &field_metas)
{
  if (inited_) { LOG_WARN("Failed to create index due to already inited. file_name:%s", file_name); return RC::RECORD_OPENNED; }
  if (field_metas.empty()) return RC::INVALID_ARGUMENT;
  Index::init(index_meta, field_metas); // 保持原接口
  fields_.clear(); fields_.reserve(field_metas.size());
  for (auto *fm : field_metas) { fields_.push_back(FieldHolder{fm->offset(), fm->len(), fm->type()}); }
  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = RC::SUCCESS;
  if (fields_.size() == 1) {
    first_field_ = *field_metas.front();
    rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, fields_[0].type, fields_[0].len);
  } else {
    std::vector<AttrType> types; types.reserve(fields_.size());
    std::vector<int> lens; lens.reserve(fields_.size());
    for (auto &f : fields_) { types.push_back(f.type); lens.push_back(f.len); }
    rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, types, lens);
  }
  if (rc != RC::SUCCESS) { LOG_WARN("Failed to create index_handler, rc=%s", strrc(rc)); return rc; }
  inited_ = true; table_ = table; LOG_INFO("Successfully create index %s", index_meta.name()); return RC::SUCCESS;
}

RC BplusTreeIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const std::vector<const FieldMeta *> &field_metas)
{
  if (inited_) { LOG_WARN("Failed to open index due to already inited. %s", file_name); return RC::RECORD_OPENNED; }
  if (field_metas.empty()) return RC::INVALID_ARGUMENT;
  Index::init(index_meta, field_metas);
  fields_.clear(); fields_.reserve(field_metas.size());
  for (auto *fm : field_metas) { fields_.push_back(FieldHolder{fm->offset(), fm->len(), fm->type()}); }
  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.open(table->db()->log_handler(), bpm, file_name);
  if (rc != RC::SUCCESS) { LOG_WARN("Failed to open index_handler rc=%s", strrc(rc)); return rc; }
  if (fields_.size() == 1) { first_field_ = *field_metas.front(); }
  inited_ = true; table_ = table; LOG_INFO("Successfully open index %s", index_meta.name()); return RC::SUCCESS;
}

RC BplusTreeIndex::close()
{
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s, field:%s", index_meta_.name(), index_meta_.field());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

RC BplusTreeIndex::insert_entry(const char *record, const RID *rid) {
  if (fields_.size() == 1) { return index_handler_.insert_entry(record + fields_[0].offset, rid); }
  int total_len = calc_composite_length(fields_);
  std::vector<char> keybuf(total_len);
  fill_composite_key(fields_, record, keybuf.data());
  return index_handler_.insert_entry(keybuf.data(), rid);
}

RC BplusTreeIndex::delete_entry(const char *record, const RID *rid) {
  if (fields_.size() == 1) { return index_handler_.delete_entry(record + fields_[0].offset, rid); }
  int total_len = calc_composite_length(fields_);
  std::vector<char> keybuf(total_len);
  fill_composite_key(fields_, record, keybuf.data());
  return index_handler_.delete_entry(keybuf.data(), rid);
}

IndexScanner *BplusTreeIndex::create_scanner(const char *left_key, int left_len, bool left_inclusive,
                                             const char *right_key, int right_len, bool right_inclusive) {
  if (fields_.size() > 1) {
    int composite_len = calc_composite_length(fields_);
    if (left_key && left_len <= 0) left_len = composite_len;
    if (right_key && right_len <= 0) right_len = composite_len;
  }
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
  if (rc != RC::SUCCESS) { LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc)); delete index_scanner; return nullptr; }
  return index_scanner;
}

RC BplusTreeIndex::sync() { return index_handler_.sync(); }

////////////////////////////////////////////////////////////////////////////////
BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler) {}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept { tree_scanner_.close(); }

RC BplusTreeIndexScanner::open(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  return tree_scanner_.open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid) { return tree_scanner_.next_entry(*rid); }

RC BplusTreeIndexScanner::destroy()
{
  delete this;
  return RC::SUCCESS;
}