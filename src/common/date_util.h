// src/common/date_util.h
#ifndef DATE_UTIL_H
#define DATE_UTIL_H

class DateUtil {
public:
  /**
   * 验证日期是否合法
   * @param year 年
   * @param month 月 (1-12)
   * @param day 日
   * @return true 如果日期合法，否则 false
   */
  static bool is_valid_date(int year, int month, int day);

  /**
   * 判断是否为闰年
   * @param year 年
   * @return true 如果是闰年，否则 false
   */
  static bool is_leap_year(int year);

  /**
   * 获取指定年月的天数
   * @param year 年
   * @param month 月 (1-12)
   * @return 该月的天数
   */
  static int days_in_month(int year, int month);

  /**
   * 将年月日转换为整数表示 (YYYYMMDD)
   * @param year 年
   * @param month 月
   * @param day 日
   * @return 整数表示的日期
   */
  static int date_to_int(int year, int month, int day);

  /**
   * 将整数表示的日期转换为年月日
   * @param date_int 整数日期 (YYYYMMDD)
   * @param year 输出年
   * @param month 输出月
   * @param day 输出日
   */
  static void int_to_date(int date_int, int &year, int &month, int &day);

  /**
   * 从字符串解析日期 (格式: "YYYY-MM-DD")
   * @param date_str 日期字符串
   * @param date_int 输出整数日期
   * @return true 如果解析成功，否则 false
   */
  static bool string_to_date(const char *date_str, int &date_int);

private:
  static const int DAYS_IN_MONTH[2][13]; // 平年和闰年的每月天数
};

#endif // DATE_UTIL_H