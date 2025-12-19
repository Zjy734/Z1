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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name"); // backward compat (single-column)
const static Json::StaticString FIELD_FIELDS("fields");         // new: array of field names

RC IndexMeta::init(const char *name, const vector<const FieldMeta *> &fields)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  if (fields.empty()) {
    LOG_ERROR("Failed to init index, empty fields.");
    return RC::INVALID_ARGUMENT;
  }

  name_  = name;
  fields_.clear();
  fields_.reserve(fields.size());
  for (auto *f : fields) {
    fields_.push_back(f->name());
  }
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  // 新格式：fields 数组
  Json::Value fields_value(Json::arrayValue);
  for (const auto &f : fields_) {
    fields_value.append(f);
  }
  json_value[FIELD_FIELDS] = std::move(fields_value);
  // 兼容旧字段
  if (!fields_.empty()) {
    json_value[FIELD_FIELD_NAME] = fields_[0];
  }
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value  = json_value[FIELD_NAME];
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  vector<const FieldMeta *> fields;
  const Json::Value &fields_arr = json_value[FIELD_FIELDS];
  if (fields_arr.isArray() && fields_arr.size() > 0) {
    for (auto &it : fields_arr) {
      if (!it.isString()) {
        LOG_ERROR("Invalid field name in index [%s]", name_value.asCString());
        return RC::INTERNAL;
      }
      const FieldMeta *f = table.field(it.asCString());
      if (nullptr == f) {
        LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), it.asCString());
        return RC::SCHEMA_FIELD_MISSING;
      }
      fields.push_back(f);
    }
  } else {
    // fallback: single column using field_name
    if (!field_value.isString()) {
      LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
          name_value.asCString(), field_value.toStyledString().c_str());
      return RC::INTERNAL;
    }
    const FieldMeta *f = table.field(field_value.asCString());
    if (nullptr == f) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields.push_back(f);
  }

  return index.init(name_value.asCString(), fields);
}

const char *IndexMeta::name() const { return name_.c_str(); }
// 兼容旧接口：返回首列
const char *IndexMeta::field() const { return fields_.empty() ? "" : fields_[0].c_str(); }

void IndexMeta::desc(ostream &os) const {
  os << "index name=" << name_ << ", fields=[";
  for (size_t i = 0; i < fields_.size(); i++) {
    if (i) os << ",";
    os << fields_[i];
  }
  os << "]";
}