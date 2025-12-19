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
// Created by Wangyunlai on 2021/5/7.
//

#include "condition_filter.h"
#include "common/log/log.h"
#include "common/value.h"
#include "common/type/date_type.h" // for DateType::parse
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include <math.h>
#include <stddef.h>

using namespace common;

ConditionFilter::~ConditionFilter() {}

DefaultConditionFilter::DefaultConditionFilter()
{
  left_.is_attr     = false;
  left_.attr_length = 0;
  left_.attr_offset = 0;

  right_.is_attr     = false;
  right_.attr_length = 0;
  right_.attr_offset = 0;
}
DefaultConditionFilter::~DefaultConditionFilter() {}

RC DefaultConditionFilter::init(const ConDesc &left, const ConDesc &right, AttrType attr_type, CompOp comp_op)
{
  if (attr_type <= AttrType::UNDEFINED || attr_type >= AttrType::MAXTYPE) {
    LOG_ERROR("Invalid condition with unsupported attribute type: %d", attr_type);
    return RC::INVALID_ARGUMENT;
  }

  if (comp_op < EQUAL_TO || comp_op >= NO_OP) {
    LOG_ERROR("Invalid condition with unsupported compare operation: %d", comp_op);
    return RC::INVALID_ARGUMENT;
  }

  left_      = left;
  right_     = right;
  attr_type_ = attr_type;
  comp_op_   = comp_op;
  return RC::SUCCESS;
}

RC DefaultConditionFilter::init(Table &table, const ConditionSqlNode &condition)
{
  const TableMeta &table_meta = table.table_meta();
  ConDesc          left;
  ConDesc          right;

  AttrType type_left  = AttrType::UNDEFINED;
  AttrType type_right = AttrType::UNDEFINED;

  if (1 == condition.left_is_attr) {
    left.is_attr                = true;
    const FieldMeta *field_left = table_meta.field(condition.left_attr.attribute_name.c_str());
    if (nullptr == field_left) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), condition.left_attr.attribute_name.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
    left.attr_length = field_left->len();
    left.attr_offset = field_left->offset();

    type_left = field_left->type();
  } else {
    left.is_attr = false;
    left.value   = condition.left_value;  // 校验type 或者转换类型
    type_left    = condition.left_value.attr_type();

    left.attr_length = 0;
    left.attr_offset = 0;
  }

  if (1 == condition.right_is_attr) {
    right.is_attr                = true;
    const FieldMeta *field_right = table_meta.field(condition.right_attr.attribute_name.c_str());
    if (nullptr == field_right) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), condition.right_attr.attribute_name.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
    right.attr_length = field_right->len();
    right.attr_offset = field_right->offset();
    type_right        = field_right->type();
  } else {
    right.is_attr = false;
    right.value   = condition.right_value;
    type_right    = condition.right_value.attr_type();

    right.attr_length = 0;
    right.attr_offset = 0;
  }

  // 校验和转换
  //  if (!field_type_compare_compatible_table[type_left][type_right]) {
  //    // 不能比较的两个字段， 要把信息传给客户端
  //    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  //  }
  // NOTE：这里没有实现不同类型的数据比较，比如整数跟浮点数之间的对比
  // 但是选手们还是要实现。这个功能在预选赛中会出现
  if (type_left != type_right) {
    // 简单兼容: int <-> float 比较，统一提升为 float
    if ((type_left == AttrType::INTS && type_right == AttrType::FLOATS) ||
        (type_left == AttrType::FLOATS && type_right == AttrType::INTS)) {
      // 统一使用 FLOATS 类型
      if (!left.is_attr) {
        if (type_left == AttrType::INTS) {
          left.value.set_float(static_cast<float>(left.value.get_int()));
        }
      }
      if (!right.is_attr) {
        if (type_right == AttrType::INTS) {
          right.value.set_float(static_cast<float>(right.value.get_int()));
        }
      }
      type_left = type_right = AttrType::FLOATS;
  } else if ((type_left == AttrType::CHARS && type_right == AttrType::INTS) ||
         (type_left == AttrType::INTS && type_right == AttrType::CHARS)) {
      // 允许数字字符串与整数比较: 将字符串尝试转换为整数
      auto convert_char_to_int = [](Value &val) -> bool {
        if (val.attr_type() != AttrType::CHARS) return false;
        std::string s = val.get_string();
        if (s.empty()) return false;
        for (char c : s) {
          if (c < '0' || c > '9') return false;
        }
        try {
          int v = std::stoi(s);
          val.set_int(v);
        } catch (...) {
          return false;
        }
        return true;
      };
      if (type_left == AttrType::CHARS && left.is_attr == false) {
        if (!convert_char_to_int(left.value)) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        type_left = AttrType::INTS;
      }
      if (type_right == AttrType::CHARS && right.is_attr == false) {
        if (!convert_char_to_int(right.value)) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        type_right = AttrType::INTS;
      }
      if (type_left != type_right) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
    } else if ((type_left == AttrType::DATES && type_right == AttrType::CHARS) ||
               (type_left == AttrType::CHARS && type_right == AttrType::DATES)) {
      // 宽松日期比较：如果是值端的 CHARS，尝试解析为合法日期；成功则提升为 DATES；失败保持 CHARS（比较阶段返回不等）
      auto try_parse_date = [](Value &val) -> bool {
        if (val.attr_type() != AttrType::CHARS) return false;
        std::string s = val.get_string();
        int days = 0;
        if (DateType::parse(s, days) == RC::SUCCESS) {
          val.set_int(days);
          val.set_type(AttrType::DATES);
          return true;
        }
        return false;
      };
      if (!left.is_attr && type_left == AttrType::CHARS && type_right == AttrType::DATES) {
        if (try_parse_date(left.value)) {
          type_left = AttrType::DATES;
        } else {
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      }
      if (!right.is_attr && type_right == AttrType::CHARS && type_left == AttrType::DATES) {
        if (try_parse_date(right.value)) {
          type_right = AttrType::DATES;
        } else {
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      }
      // 若仍不一致，允许进入比较(交给 Value::compare 的 DATE vs CHARS 分支)——不返回错误
    } else {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  return init(left, right, type_left, condition.comp);
}

bool DefaultConditionFilter::filter(const Record &rec) const
{
  Value left_value;
  Value right_value;

  if (left_.is_attr) {  // value
    left_value.set_type(attr_type_);
    left_value.set_data(rec.data() + left_.attr_offset, left_.attr_length);
  } else {
    left_value.set_value(left_.value);
  }

  if (right_.is_attr) {
    right_value.set_type(attr_type_);
    right_value.set_data(rec.data() + right_.attr_offset, right_.attr_length);
  } else {
    right_value.set_value(right_.value);
  }

  // LIKE 处理
  auto like_match = [](const string &text, const string &pattern) -> bool {
    // 支持 % 与 _，且不匹配单引号字符 '\''
    // 动态规划通配匹配
    size_t n = text.size();
    size_t m = pattern.size();
    vector<vector<char>> dp(n + 1, vector<char>(m + 1, 0));
    dp[0][0] = 1;
    for (size_t j = 1; j <= m; ++j) {
      if (pattern[j - 1] == '%') dp[0][j] = dp[0][j - 1];
    }
    auto char_match = [](char tc, char pc) -> bool {
      if (pc == '_') {
        return tc != '\''; // '_' 不匹配单引号
      }
      return tc == pc;
    };
  for (size_t i = 1; i <= n; ++i) {
      for (size_t j = 1; j <= m; ++j) {
        if (pattern[j - 1] == '%') {
      // % 匹配任意长度(含0)，但不匹配单引号字符
      dp[i][j] = dp[i][j - 1] || ((text[i - 1] != '\'') && dp[i - 1][j]);
        } else if (char_match(text[i - 1], pattern[j - 1])) {
          dp[i][j] = dp[i - 1][j - 1];
        } else {
          dp[i][j] = 0;
        }
      }
    }
    return dp[n][m];
  };

  int cmp_result = left_value.compare(right_value);

  switch (comp_op_) {
    case EQUAL_TO: return 0 == cmp_result;
    case LESS_EQUAL: return cmp_result <= 0;
    case NOT_EQUAL: return cmp_result != 0;
    case LESS_THAN: return cmp_result < 0;
    case GREAT_EQUAL: return cmp_result >= 0;
    case GREAT_THAN: return cmp_result > 0;
    case LIKE_OP: {
      if (attr_type_ != AttrType::CHARS) {
        // 仅支持char
        return false;
      }
      string text  = left_value.get_string();
      string pat   = right_value.get_string();
      return like_match(text, pat);
    }
    case NOT_LIKE_OP: {
      if (attr_type_ != AttrType::CHARS) {
        return false;
      }
      string text  = left_value.get_string();
      string pat   = right_value.get_string();
      return !like_match(text, pat);
    }

    default: break;
  }

  LOG_PANIC("Never should print this.");
  return cmp_result;  // should not go here
}

CompositeConditionFilter::~CompositeConditionFilter()
{
  if (memory_owner_) {
    delete[] filters_;
    filters_ = nullptr;
  }
}

RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num, bool own_memory)
{
  filters_      = filters;
  filter_num_   = filter_num;
  memory_owner_ = own_memory;
  return RC::SUCCESS;
}
RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num)
{
  return init(filters, filter_num, false);
}

RC CompositeConditionFilter::init(Table &table, const ConditionSqlNode *conditions, int condition_num)
{
  if (condition_num == 0) {
    return RC::SUCCESS;
  }
  if (conditions == nullptr) {
    return RC::INVALID_ARGUMENT;
  }

  RC                rc                = RC::SUCCESS;
  ConditionFilter **condition_filters = new ConditionFilter *[condition_num];
  for (int i = 0; i < condition_num; i++) {
    DefaultConditionFilter *default_condition_filter = new DefaultConditionFilter();
    rc                                               = default_condition_filter->init(table, conditions[i]);
    if (rc != RC::SUCCESS) {
      delete default_condition_filter;
      for (int j = i - 1; j >= 0; j--) {
        delete condition_filters[j];
        condition_filters[j] = nullptr;
      }
      delete[] condition_filters;
      condition_filters = nullptr;
      return rc;
    }
    condition_filters[i] = default_condition_filter;
  }
  return init((const ConditionFilter **)condition_filters, condition_num, true);
}

bool CompositeConditionFilter::filter(const Record &rec) const
{
  for (int i = 0; i < filter_num_; i++) {
    if (!filters_[i]->filter(rec)) {
      return false;
    }
  }
  return true;
}