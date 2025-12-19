/* Copyright (c) OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
*/

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    // no child means full table scan is not provided; but for safety just return SUCCESS
    return RC::SUCCESS;
  }

  auto &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  trx_ = trx;

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current tuple while collecting for update");
      return RC::INTERNAL;
    }
    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    records_.emplace_back(std::move(record));
  }
  child->close();

  // perform updates
  for (Record &old_rec : records_) {
    Record new_rec;
    rc = build_updated_record(old_rec, new_rec);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to build updated record. rc=%s", strrc(rc));
      return rc;
    }
    rc = trx_->update_record(table_, old_rec, new_rec);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to update record by transaction. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::build_updated_record(const Record &old_record, Record &new_record)
{
  // copy full row then overwrite target field with casted value
  RC rc = new_record.copy_data(old_record.data(), old_record.len());
  if (OB_FAIL(rc)) {
    return rc;
  }
  new_record.set_rid(old_record.rid());

  Value real_value;
  if (value_.attr_type() != field_meta_->type()) {
    rc = Value::cast_to(value_, field_meta_->type(), real_value);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to cast value for update. field=%s target=%d rc=%s",
               field_meta_->name(), (int)field_meta_->type(), strrc(rc));
      return rc;
    }
  } else {
    real_value = value_;
  }

  size_t copy_len = field_meta_->len();
  size_t data_len = real_value.length();
  if (field_meta_->type() == AttrType::CHARS) {
    if (copy_len > data_len) {
      copy_len = data_len + 1; // reserve null terminator as in insert
    }
  }
  memcpy(new_record.data() + field_meta_->offset(), real_value.data(), copy_len);
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }

RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }