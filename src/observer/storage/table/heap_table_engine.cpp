/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/table/heap_table_engine.h"
#include "storage/record/heap_record_scanner.h"
#include "common/log/log.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/common/meta_util.h"
#include "storage/db/db.h"


HeapTableEngine::~HeapTableEngine()
{
  if (record_handler_ != nullptr) {
    delete record_handler_;
    record_handler_ = nullptr;
  }

  if (data_buffer_pool_ != nullptr) {
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
  }

  for (vector<Index *>::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
    Index *index = *it;
    delete index;
  }
  indexes_.clear();
  LOG_INFO("Table has been closed: %s", table_meta_->name());
}

RC HeapTableEngine::insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->insert_record(record.data(), table_meta_->record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                table_meta_->name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                table_meta_->name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

RC HeapTableEngine::insert_chunk(const Chunk& chunk)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->insert_chunk(chunk, table_meta_->record_size());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert chunk failed. table name=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  // TODO: insert chunk support update index
  return rc;
}

RC HeapTableEngine::update_record_with_trx(const Record &old_record, const Record &new_record, Trx *trx)
{
  // Heap engine: maintain indexes and update in-place via record handler visit.
  // Only touch indexes whose key actually changes to avoid duplicate key on unchanged indexes.
  RC rc = RC::SUCCESS;

  const int rec_len = table_meta_->record_size();
  // Copy old row data before any in-place write (old_record.data() may point to page memory)
  std::vector<char> old_data(rec_len);
  memcpy(old_data.data(), old_record.data(), rec_len);

  // Determine which indexes are affected (multi-column aware)
  vector<Index *> changed_indexes;
  changed_indexes.reserve(indexes_.size());
  for (Index *index : indexes_) {
    bool changed = false;
    for (const string &fname : index->index_meta().fields()) {
      const FieldMeta *fm = table_meta_->field(fname.c_str());
      if (fm == nullptr) { LOG_ERROR("invalid index meta: field not found. table=%s index=%s field=%s",
                table_meta_->name(), index->index_meta().name(), fname.c_str()); return RC::INTERNAL; }
      int off = fm->offset(); int len = fm->len();
      if (0 != memcmp(old_data.data() + off, new_record.data() + off, len)) { changed = true; break; }
    }
    if (changed) changed_indexes.push_back(index);
  }

  // Insert new index entries for changed indexes
  for (Index *index : changed_indexes) {
    rc = index->insert_entry(new_record.data(), const_cast<RID *>(&old_record.rid()));
    if (rc != RC::SUCCESS) {
      // rollback previously inserted new entries
      for (Index *rindex : changed_indexes) {
        if (rindex == index) break;
        RC rc2 = rindex->delete_entry(new_record.data(), const_cast<RID *>(&old_record.rid()));
        if (rc2 != RC::SUCCESS) {
          LOG_ERROR("failed to rollback new index entry on update. table=%s index=%s rc=%s",
                    table_meta_->name(), rindex->index_meta().name(), strrc(rc2));
        }
      }
      return rc;
    }
  }

  // Write new record content in place using visit_record to ensure proper logging and latching
  rc = record_handler_->visit_record(old_record.rid(), [&](Record &rec) -> bool {
    // rec owns its memory (copy), just overwrite
    memcpy(rec.data(), new_record.data(), rec_len);
    return true;
  });
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to update record data. table=%s rid=%s rc=%s",
              table_meta_->name(), old_record.rid().to_string().c_str(), strrc(rc));
    // remove newly inserted index entries to rollback
    for (Index *index : changed_indexes) {
      RC rc2 = index->delete_entry(new_record.data(), const_cast<RID *>(&old_record.rid()));
      if (rc2 != RC::SUCCESS) {
        LOG_ERROR("failed to rollback new index after record write fail. table=%s index=%s rc=%s",
                  table_meta_->name(), index->index_meta().name(), strrc(rc2));
      }
    }
    return rc;
  }

  // Delete old index entries (use copied old_data to avoid reading mutated page memory)
  for (Index *index : changed_indexes) {
    rc = index->delete_entry(old_data.data(), const_cast<RID *>(&old_record.rid()));
    if (rc != RC::SUCCESS) {
      LOG_ERROR("failed to delete old index entry on update. table=%s index=%s rc=%s",
                table_meta_->name(), index->index_meta().name(), strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC HeapTableEngine::visit_record(const RID &rid, function<bool(Record &)> visitor)
{
  return record_handler_->visit_record(rid, visitor);
}

RC HeapTableEngine::get_record(const RID &rid, Record &record)
{
  RC rc = record_handler_->get_record(rid, record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to visit record. rid=%s, table=%s, rc=%s", rid.to_string().c_str(), table_meta_->name(), strrc(rc));
    return rc;
  }

  return rc;
}

RC HeapTableEngine::delete_record(const Record &record)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record.data(), &record.rid());
    ASSERT(RC::SUCCESS == rc, 
           "failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
           table_meta_->name(), index->index_meta().name(), record.rid().to_string().c_str(), strrc(rc));
  }
  rc = record_handler_->delete_record(&record.rid());
  return rc;
}

RC HeapTableEngine::get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode)
{
  scanner = new HeapRecordScanner(table_, *data_buffer_pool_, trx, db_->log_handler(), mode, nullptr);
  RC rc = scanner->open_scan();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
    delete scanner;
    scanner = nullptr;
  }
  return rc;
}

RC HeapTableEngine::get_chunk_scanner(ChunkFileScanner &scanner, Trx *trx, ReadWriteMode mode)
{
  RC rc = scanner.open_scan_chunk(table_, *data_buffer_pool_, db_->log_handler(), mode);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC HeapTableEngine::create_index(Trx *trx, const vector<const FieldMeta *> &field_metas, const char *index_name)
{
  if (common::is_blank(index_name) || field_metas.empty()) {
    LOG_INFO("Invalid input arguments, table name is %s, index_name is blank or fields empty", table_meta_->name());
    return RC::INVALID_ARGUMENT;
  }

  IndexMeta new_index_meta;
  RC rc = new_index_meta.init(index_name, field_metas);
  if (rc != RC::SUCCESS) {
    LOG_INFO("Failed to init IndexMeta in table:%s, index_name:%s", table_meta_->name(), index_name);
    return rc;
  }

  // 1. 创建索引对象和文件
  BplusTreeIndex *index = new BplusTreeIndex();
  string index_file = table_index_file(db_->path().c_str(), table_meta_->name(), index_name);

  rc = index->create(table_, index_file.c_str(), new_index_meta, field_metas);
  if (rc != RC::SUCCESS) {
    delete index;
    LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
    return rc;
  }

  // 2. 遍历当前的所有数据，插入这个索引
  RecordScanner *scanner = nullptr;
  rc = get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s", table_meta_->name(),
             index_name, strrc(rc));
    delete index; // 清理已创建的索引
    return rc;
  }

  Record record;
  while (true) {
    rc = scanner->next(record);
    if (rc == RC::RECORD_EOF) {
      rc = RC::SUCCESS;
      break; // 扫描完成
    }
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get next record while creating index. table=%s, index=%s, rc=%s", table_meta_->name(),
               index_name, strrc(rc));
      break;
    }
    RC insert_rc = index->insert_entry(record.data(), &record.rid());
    if (insert_rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
               table_meta_->name(), index_name, strrc(insert_rc));
      rc = insert_rc;
      break;
    }
  }

  // 清理 scanner
  scanner->close_scan();
  delete scanner;
  scanner = nullptr;

  if (rc != RC::SUCCESS) {
    delete index; // 如果填充失败，清理索引
    return rc;
  }

  LOG_INFO("inserted all records into new index. table=%s, index=%s", table_meta_->name(), index_name);

  // 3. 更新元数据
  indexes_.push_back(index);

  TableMeta new_table_meta(*table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, table_meta_->name(), rc, strrc(rc));
    indexes_.pop_back(); // 从内存中移除
    delete index;        // 删除索引对象
    return rc;
  }

  string tmp_file = table_meta_file(db_->path().c_str(), table_meta_->name()) + ".tmp";
  fstream fs;
  fs.open(tmp_file, ios_base::out | ios_base::binary | ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    indexes_.pop_back();
    delete index;
    return RC::IOERR_OPEN;
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    fs.close();
    indexes_.pop_back();
    delete index;
    return RC::IOERR_WRITE;
  }
  fs.close();

  string meta_file = table_meta_file(db_->path().c_str(), table_meta_->name());
  int ret = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). "
              "system error=%d:%s",
              tmp_file.c_str(), meta_file.c_str(), index_name, table_meta_->name(), errno, strerror(errno));
    indexes_.pop_back();
    delete index;
    return RC::IOERR_WRITE;
  }

  table_meta_->swap(new_table_meta);

  LOG_INFO("Successfully added a new index (%s) on the table (%s)", index_name, table_meta_->name());
  return RC::SUCCESS;
}

RC HeapTableEngine::insert_entry_of_indexes(const char *record, const RID &rid)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC HeapTableEngine::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

RC HeapTableEngine::sync()
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
          table_meta_->name(),
          index->index_meta().name(),
          rc,
          strrc(rc));
      return rc;
    }
  }

  rc = data_buffer_pool_->flush_all_pages();
  LOG_INFO("Sync table over. table=%s", table_meta_->name());
  return rc;
}

Index *HeapTableEngine::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}
Index *HeapTableEngine::find_index_by_field(const char *field_name) const
{
  const IndexMeta *index_meta = table_meta_->find_index_by_field(field_name);
  if (index_meta != nullptr) {
    return this->find_index(index_meta->name());
  }
  return nullptr;
}

RC HeapTableEngine::init()
{
  string data_file = table_data_file(db_->path().c_str(), table_meta_->name());

  BufferPoolManager &bpm = db_->buffer_pool_manager();
  RC                 rc  = bpm.open_file(db_->log_handler(), data_file.c_str(), data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s", data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler(table_meta_->storage_format());

  rc = record_handler_->init(*data_buffer_pool_, db_->log_handler(), table_meta_, table_->lob_handler_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%s", strrc(rc));
    delete record_handler_;
    record_handler_ = nullptr;
    return rc;
  }

  return rc;
}

RC HeapTableEngine::open()
{
  RC rc = RC::SUCCESS;
  init();
  const int index_num = table_meta_->index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_->index(i);
    // rebuild field metas for this index
    vector<const FieldMeta *> metas;
    metas.reserve(index_meta->fields().size());
    for (const string &fn : index_meta->fields()) {
      const FieldMeta *fm = table_meta_->field(fn.c_str());
      if (fm == nullptr) { metas.clear(); break; }
      metas.push_back(fm);
    }
    if (metas.empty()) {
      LOG_ERROR("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
                table_meta_->name(), index_meta->name(), index_meta->field());
      // skip cleanup
      //  do all cleanup action in destructive Table function
      return RC::INTERNAL;
    }

    BplusTreeIndex *index      = new BplusTreeIndex();
    string          index_file = table_index_file(db_->path().c_str(), table_meta_->name(), index_meta->name());

  // 从 meta 重建字段列表（顺序）
  rc = index->open(table_, index_file.c_str(), *index_meta, metas);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%s",
                table_meta_->name(), index_meta->name(), index_file.c_str(), strrc(rc));
      // skip cleanup
      //  do all cleanup action in destructive Table function.
      return rc;
    }
    indexes_.push_back(index);
  }
  return rc;
}