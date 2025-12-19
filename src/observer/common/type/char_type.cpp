/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include "common/type/date_type.h"

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::INTS: {
      std::string s(val.value_.pointer_value_, val.length_);
      try {
        size_t pos;
        int int_val = std::stoi(s, &pos);
        // 确保整个字符串都被解析了
        if (pos != s.length()) {
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
        result.set_int(int_val);
      } catch (const std::invalid_argument &) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      } catch (const std::out_of_range &) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      return RC::SUCCESS;
    }
    case AttrType::FLOATS: {
      std::string s(val.value_.pointer_value_, val.length_);
      try {
        size_t pos;
        float float_val = std::stof(s, &pos);
        // 确保整个字符串都被解析了
        if (pos != s.length()) {
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
        result.set_float(float_val);
      } catch (const std::invalid_argument &) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      } catch (const std::out_of_range &) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      return RC::SUCCESS;
    }
    case AttrType::DATES: {
      // 从字符串解析为日期，接受 YYYY-M-D/YYYY-MM-DD 等变体
      int days = 0;
      // 直接使用底层缓冲区内容，避免经由 to_string 带来歧义
      std::string s(val.value_.pointer_value_ ? val.value_.pointer_value_ : "");
      // 如果底层长度已知，按长度截断，避免尾部垃圾或内嵌\0问题
      if (!s.empty() && (int)s.size() != val.length_) {
        s.assign(val.value_.pointer_value_, val.length_);
      }
      RC rc = DateType::parse(s, days);
      if (OB_FAIL(rc)) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      result.reset();
      result.set_type(AttrType::DATES);
      result.set_int(days);
      result.set_type(AttrType::DATES);
      return RC::SUCCESS;
    }
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  if (type == AttrType::DATES || type == AttrType::INTS || type == AttrType::FLOATS) {
    // 允许从字符串到日期的隐式转换
    return 1;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  // 返回真实字符串内容，而不是指针地址
  if (val.value_.pointer_value_ == nullptr || val.length_ <= 0) {
    result.clear();
  } else {
    result.assign(val.value_.pointer_value_, val.length_);
  }
  return RC::SUCCESS;
}