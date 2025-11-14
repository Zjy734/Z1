// src/common/date_util.cpp
#include "date_util.h"
#include <cstdio>
#include <cstring>

const int DateUtil::DAYS_IN_MONTH[2][13] = {
  {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}, // 平年
  {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}  // 闰年
};

bool DateUtil::is_valid_date(int year, int month, int day) {
  if (year < 0 || year > 9999 || month < 1 || month > 12 || day < 1) {
    return false;
  }
  
  int max_days = days_in_month(year, month);
  return day <= max_days;
}

bool DateUtil::is_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

int DateUtil::days_in_month(int year, int month) {
  return DAYS_IN_MONTH[is_leap_year(year) ? 1 : 0][month];
}

int DateUtil::date_to_int(int year, int month, int day) {
  return year * 10000 + month * 100 + day;
}

void DateUtil::int_to_date(int date_int, int &year, int &month, int &day) {
  year = date_int / 10000;
  month = (date_int % 10000) / 100;
  day = date_int % 100;
}

bool DateUtil::string_to_date(const char *date_str, int &date_int) {
  int year, month, day;
  if (sscanf(date_str, "%d-%d-%d", &year, &month, &day) != 3) {
    return false;
  }
  
  if (!is_valid_date(year, month, day)) {
    return false;
  }
  
  date_int = date_to_int(year, month, day);
  return true;
}