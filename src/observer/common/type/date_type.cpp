#include "common/type/date_type.h"
#include "common/value.h"
#include "storage/common/column.h"
#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/lang/sstream.h"
#include <regex>

static const int EPOCH_YEAR = 1970; // base year

bool DateType::leap_year(int year) {
  if (year % 400 == 0) return true;
  if (year % 100 == 0) return false;
  return year % 4 == 0;
}

bool DateType::valid_date(int year, int month, int day) {
  if (year < 1 || year > 9999) return false;
  if (month < 1 || month > 12) return false;
  static const int mdays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
  int dim = mdays[month-1];
  if (month == 2 && leap_year(year)) dim = 29;
  return day >= 1 && day <= dim;
}

RC DateType::parse(const string &text, int &days) {
  // 支持 YYYY-M-D / YYYY-MM-D / YYYY-M-DD / YYYY-MM-DD
  int hy1 = -1, hy2 = -1;
  for (int i = 0; i < (int)text.size(); ++i) {
    if (text[i] == '-') {
      if (hy1 == -1) hy1 = i; else { hy2 = i; break; }
    }
  }
  if (hy1 <= 0 || hy2 <= hy1 + 1) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  if (hy2 >= (int)text.size() - 1) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  string sy = text.substr(0, hy1);
  string sm = text.substr(hy1 + 1, hy2 - hy1 - 1);
  string sd = text.substr(hy2 + 1);
  int year = 0, month = 0, day = 0;
  try {
    // 限制年份为1-9999，位数1-4都可
    if (sy.empty() || sm.empty() || sd.empty()) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    for (char c : sy) if (c < '0' || c > '9') return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    for (char c : sm) if (c < '0' || c > '9') return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    for (char c : sd) if (c < '0' || c > '9') return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    year = stoi(sy);
    month = stoi(sm);
    day = stoi(sd);
  } catch (...) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  if (!valid_date(year, month, day)) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  // days offset from 1970-01-01
  // accumulate years
  long long total = 0; // can be negative
  if (year >= EPOCH_YEAR) {
    for (int y = EPOCH_YEAR; y < year; ++y) {
      total += leap_year(y) ? 366 : 365;
    }
  } else {
    for (int y = year; y < EPOCH_YEAR; ++y) {
      total -= leap_year(y) ? 366 : 365;
    }
  }
  static const int mdays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
  for (int m = 1; m < month; ++m) {
    int dim = mdays[m-1];
    if (m == 2 && leap_year(year)) dim = 29;
    total += dim;
  }
  total += (day - 1); // 1970-01-01 -> 0
  days = (int)total;
  return RC::SUCCESS;
}

void DateType::days_to_date(int days, int &year, int &month, int &day) {
  // convert days offset back to date
  year = EPOCH_YEAR;
  long long d = days;
  if (d >= 0) {
    while (true) {
      int year_days = leap_year(year) ? 366 : 365;
      if (d >= year_days) { d -= year_days; ++year; } else break;
    }
  } else {
    while (d < 0) {
      int prev_year = year - 1;
      int year_days = leap_year(prev_year) ? 366 : 365;
      d += year_days;
      year = prev_year;
    }
  }
  static const int mdays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
  month = 1;
  for (int i = 0; i < 12; ++i) {
    int dim = mdays[i];
    if (i == 1 && leap_year(year)) dim = 29;
    if (d >= dim) { d -= dim; ++month; } else break;
  }
  day = (int)d + 1;
}

int DateType::compare(const Value &left, const Value &right) const {
  ASSERT(left.attr_type() == AttrType::DATES, "left type is not date");
  ASSERT(right.attr_type() == AttrType::DATES, "right type is not date");
  int l = left.get_int();
  int r = right.get_int();
  if (l < r) return -1;
  if (l > r) return 1;
  return 0;
}

int DateType::compare(const Column &left, const Column &right, int left_idx, int right_idx) const {
  ASSERT(left.attr_type() == AttrType::DATES, "left type is not date");
  ASSERT(right.attr_type() == AttrType::DATES, "right type is not date");
  int l = ((int*)left.data())[left_idx];
  int r = ((int*)right.data())[right_idx];
  if (l < r) return -1;
  if (l > r) return 1;
  return 0;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const {
  switch (type) {
    case AttrType::INTS: {
      result.set_int(val.get_int()); return RC::SUCCESS;
    }
    case AttrType::FLOATS: {
      result.set_float((float)val.get_int()); return RC::SUCCESS;
    }
    case AttrType::CHARS: {
      string s; to_string(val, s); result.set_string(s.c_str()); return RC::SUCCESS;
    }
    case AttrType::DATES: {
      result.set_int(val.get_int()); result.set_type(AttrType::DATES); return RC::SUCCESS; // copy
    }
    default: return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

int DateType::cast_cost(AttrType type) {
  if (type == AttrType::DATES) return 0;
  if (type == AttrType::INTS || type == AttrType::FLOATS) return 1;
  if (type == AttrType::CHARS) return 2; // 与 CHARS 之间更偏向 CHARS -> DATES 的方向
  return INT32_MAX;
}

RC DateType::to_string(const Value &val, string &result) const {
  int year, month, day; days_to_date(val.get_int(), year, month, day);
  char buf[16]; snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
  result = buf; return RC::SUCCESS;
}

RC DateType::set_value_from_str(Value &val, const string &data) const {
  int days; RC rc = parse(data, days); if (OB_FAIL(rc)) return rc; val.reset(); val.set_type(AttrType::DATES); val.set_int(days); val.set_type(AttrType::DATES); return RC::SUCCESS; }