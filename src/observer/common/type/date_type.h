/*
 * DateType 定义: 以自1970-01-01的天数(int32_t)存储，可以为负数。
 */
#pragma once

#include "common/type/data_type.h"

class DateType : public DataType {
public:
  DateType() : DataType(AttrType::DATES) {}
  virtual ~DateType() {}

  int compare(const Value &left, const Value &right) const override;
  int compare(const Column &left, const Column &right, int left_idx, int right_idx) const override;

  RC cast_to(const Value &val, AttrType type, Value &result) const override;
  int cast_cost(AttrType type) override;

  RC to_string(const Value &val, string &result) const override;
  RC set_value_from_str(Value &val, const string &data) const override;

  // 解析 YYYY-MM-DD 字符串为天数，非法返回 RC::SCHEMA_FIELD_TYPE_MISMATCH
  static RC parse(const string &text, int &days);
  static bool valid_date(int year, int month, int day);
  static bool leap_year(int year);

  static void days_to_date(int days, int &year, int &month, int &day);
};