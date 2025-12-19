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
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/integer_type.h"
#include "common/value.h"
#include "storage/common/column.h"

#include <cmath>
#include <limits>

int IntegerType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::INTS, "left type is not integer");
  ASSERT(right.attr_type() == AttrType::INTS || right.attr_type() == AttrType::FLOATS, "right type is not numeric");
  if (right.attr_type() == AttrType::INTS) {
    return common::compare_int((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
  } else if (right.attr_type() == AttrType::FLOATS) {
    float left_val  = left.get_float();
    float right_val = right.get_float();
    return common::compare_float((void *)&left_val, (void *)&right_val);
  }
  return INT32_MAX;
}

int IntegerType::compare(const Column &left, const Column &right, int left_idx, int right_idx) const
{
  ASSERT(left.attr_type() == AttrType::INTS, "left type is not integer");
  ASSERT(right.attr_type() == AttrType::INTS, "right type is not integer");
  return common::compare_int((void *)&((int*)left.data())[left_idx],
      (void *)&((int*)right.data())[right_idx]);
}

RC IntegerType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
  case AttrType::FLOATS: {
    float float_value = val.get_int();
    result.set_float(float_value);
    return RC::SUCCESS;
  }
  default:
    LOG_WARN("unsupported type %d", type);
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

RC IntegerType::add(const Value &left, const Value &right, Value &result) const
{
  result.set_int(left.get_int() + right.get_int());
  return RC::SUCCESS;
}

RC IntegerType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_int(left.get_int() - right.get_int());
  return RC::SUCCESS;
}

RC IntegerType::multiply(const Value &left, const Value &right, Value &result) const
{
  result.set_int(left.get_int() * right.get_int());
  return RC::SUCCESS;
}

RC IntegerType::divide(const Value &left, const Value &right, Value &result) const
{
  const int rv = right.get_int();

  // 以 NaN 模拟 NULL-like：与 FloatType::divide 保持一致。
  // 这里不返回 RC 错误，避免表达式整体失败；NULL-like 值后续在 WHERE 中会被当作 false 过滤掉。
  if (rv == 0) {
    result.set_type(AttrType::FLOATS);
    result.set_float(std::numeric_limits<float>::quiet_NaN());
    return RC::SUCCESS;
  }

  // 为了和常见数据库一致：整型 / 整型，结果可能是小数，使用 float 表示
  result.set_type(AttrType::FLOATS);
  result.set_float(static_cast<float>(left.get_int()) / static_cast<float>(rv));
  return RC::SUCCESS;
}

RC IntegerType::negative(const Value &val, Value &result) const
{
  result.set_int(-val.get_int());
  return RC::SUCCESS;
}

RC IntegerType::set_value_from_str(Value &val, const string &data) const
{
  RC                rc = RC::SUCCESS;
  stringstream deserialize_stream;
  deserialize_stream.clear();  // 清理stream的状态，防止多次解析出现异常
  deserialize_stream.str(data);
  int int_value;
  deserialize_stream >> int_value;
  if (!deserialize_stream || !deserialize_stream.eof()) {
    rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
  } else {
    val.set_int(int_value);
  }
  return rc;
}

RC IntegerType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.int_value_;
  result = ss.str();
  return RC::SUCCESS;
}