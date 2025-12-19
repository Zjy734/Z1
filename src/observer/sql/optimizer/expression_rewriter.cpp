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
// Created by Wangyunlai on 2022/12/13.
//

#include "sql/optimizer/expression_rewriter.h"
#include "common/log/log.h"
#include "sql/optimizer/comparison_simplification_rule.h"
#include "sql/optimizer/conjunction_simplification_rule.h"

using namespace std;

ExpressionRewriter::ExpressionRewriter()
{
  expr_rewrite_rules_.emplace_back(new ComparisonSimplificationRule);
  expr_rewrite_rules_.emplace_back(new ConjunctionSimplificationRule);
  // 扩展：算术比较重写，支持 (F - K) op C 与 C op (F - K) 转换为 F op' (C + K)
  class ArithmeticComparisonRewriteRule : public ExpressionRewriteRule {
  public:
    RC rewrite(unique_ptr<Expression> &expr, bool &change_made) override {
      change_made = false;
      if (expr->type() != ExprType::COMPARISON) {
        return RC::SUCCESS;
      }
      auto cmp_expr = static_cast<ComparisonExpr *>(expr.get());
      CompOp op = cmp_expr->comp();
      if (!(op == LESS_THAN || op == LESS_EQUAL || op == GREAT_THAN || op == GREAT_EQUAL)) {
        return RC::SUCCESS; // 仅处理范围比较
      }
      Expression *L = cmp_expr->left().get();
      Expression *R = cmp_expr->right().get();

      auto match_field_sub_const = [](Expression *e, FieldExpr *&field_out, ValueExpr *&const_out) -> bool {
        if (e->type() != ExprType::ARITHMETIC) return false;
        auto arith = static_cast<ArithmeticExpr *>(e);
        if (arith->arithmetic_type() != ArithmeticExpr::Type::SUB) return false;
        Expression *sub_l = arith->left().get();
        Expression *sub_r = arith->right().get();
        if (sub_l->type() != ExprType::FIELD || sub_r->type() != ExprType::VALUE) return false;
        field_out = static_cast<FieldExpr *>(sub_l);
        const_out = static_cast<ValueExpr *>(sub_r);
        return true;
      };

      auto to_float = [](const Value &v, float &out, AttrType &origin) -> bool {
        origin = v.attr_type();
        if (origin != AttrType::INTS && origin != AttrType::FLOATS) return false;
        Value cast_v = v;
        if (origin == AttrType::INTS) { Value::cast_to(cast_v, AttrType::FLOATS, cast_v); }
        out = cast_v.get_float();
        return true;
      };

      // (F - K) op C  ==> F op (C + K)
      FieldExpr *f1 = nullptr; ValueExpr *k1 = nullptr;
      if (match_field_sub_const(L, f1, k1) && R->type() == ExprType::VALUE) {
        auto c_val_expr = static_cast<ValueExpr *>(R);
        const Value &c_val = c_val_expr->get_value();
        const Value &k_val = k1->get_value();
        float c_f, k_f; AttrType c_type, k_type;
        if (!to_float(c_val, c_f, c_type) || !to_float(k_val, k_f, k_type)) {
          return RC::SUCCESS;
        }
        float new_const_f = c_f + k_f;
        CompOp new_op;
        switch (op) {
          case GREAT_THAN: new_op = GREAT_THAN; break;
          case GREAT_EQUAL: new_op = GREAT_EQUAL; break;
          case LESS_THAN: new_op = LESS_THAN; break;
          case LESS_EQUAL: new_op = LESS_EQUAL; break;
          default: return RC::SUCCESS;
        }
        Value new_const;
        if (c_type == AttrType::INTS && k_type == AttrType::INTS) {
          new_const.set_int(static_cast<int>(new_const_f + 0.0000001f));
        } else {
          new_const.set_float(new_const_f);
        }
        unique_ptr<Expression> new_field(f1->copy());
        unique_ptr<Expression> new_const_expr(new ValueExpr(new_const));
        expr.reset(new ComparisonExpr(new_op, std::move(new_field), std::move(new_const_expr)));
        change_made = true;
        LOG_TRACE("rewrite (F - K) op C ==> F op (C + K)");
        return RC::SUCCESS;
      }

      // C op (F - K) ==> F op' (C + K)
      FieldExpr *f2 = nullptr; ValueExpr *k2 = nullptr;
      if (L->type() == ExprType::VALUE && match_field_sub_const(R, f2, k2)) {
        auto c_val_expr = static_cast<ValueExpr *>(L);
        const Value &c_val = c_val_expr->get_value();
        const Value &k_val = k2->get_value();
        float c_f, k_f; AttrType c_type, k_type;
        if (!to_float(c_val, c_f, c_type) || !to_float(k_val, k_f, k_type)) {
          return RC::SUCCESS;
        }
        float new_const_f = c_f + k_f;
        CompOp new_op;
        switch (op) {
          case LESS_THAN: new_op = GREAT_THAN; break;
          case LESS_EQUAL: new_op = GREAT_EQUAL; break;
          case GREAT_THAN: new_op = LESS_THAN; break;
          case GREAT_EQUAL: new_op = LESS_EQUAL; break;
          default: return RC::SUCCESS;
        }
        Value new_const;
        if (c_type == AttrType::INTS && k_type == AttrType::INTS) {
          new_const.set_int(static_cast<int>(new_const_f + 0.0000001f));
        } else {
          new_const.set_float(new_const_f);
        }
        unique_ptr<Expression> new_field(f2->copy());
        unique_ptr<Expression> new_const_expr(new ValueExpr(new_const));
        expr.reset(new ComparisonExpr(new_op, std::move(new_field), std::move(new_const_expr)));
        change_made = true;
        LOG_TRACE("rewrite C op (F - K) ==> F op' (C + K)");
        return RC::SUCCESS;
      }
      return RC::SUCCESS;
    }
  };
  expr_rewrite_rules_.emplace_back(new ArithmeticComparisonRewriteRule);
}

RC ExpressionRewriter::rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  RC rc = RC::SUCCESS;

  bool sub_change_made = false;

  vector<unique_ptr<Expression>> &expressions = oper->expressions();
  for (unique_ptr<Expression> &expr : expressions) {
    rc = rewrite_expression(expr, sub_change_made);
    if (rc != RC::SUCCESS) {
      break;
    }

    if (sub_change_made && !change_made) {
      change_made = true;
    }
  }

  if (rc != RC::SUCCESS) {
    return rc;
  }

  vector<unique_ptr<LogicalOperator>> &child_opers = oper->children();
  for (unique_ptr<LogicalOperator> &child_oper : child_opers) {
    bool sub_change_made = false;
    rc                   = rewrite(child_oper, sub_change_made);
    if (sub_change_made && !change_made) {
      change_made = true;
    }
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC ExpressionRewriter::rewrite_expression(unique_ptr<Expression> &expr, bool &change_made)
{
  RC rc = RC::SUCCESS;

  change_made = false;
  for (unique_ptr<ExpressionRewriteRule> &rule : expr_rewrite_rules_) {
    bool sub_change_made = false;

    rc                   = rule->rewrite(expr, sub_change_made);
    if (sub_change_made && !change_made) {
      change_made = true;
    }
    if (rc != RC::SUCCESS) {
      break;
    }
  }

  if (change_made || rc != RC::SUCCESS) {
    return rc;
  }

  switch (expr->type()) {
    case ExprType::FIELD:
    case ExprType::VALUE: {
      // do nothing
    } break;

    case ExprType::CAST: {
      unique_ptr<Expression> &child_expr = (static_cast<CastExpr *>(expr.get()))->child();

      rc                                      = rewrite_expression(child_expr, change_made);
    } break;

    case ExprType::COMPARISON: {
      auto                         comparison_expr = static_cast<ComparisonExpr *>(expr.get());

      unique_ptr<Expression> &left_expr       = comparison_expr->left();
      unique_ptr<Expression> &right_expr      = comparison_expr->right();

      bool left_change_made = false;

      rc                    = rewrite_expression(left_expr, left_change_made);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      bool right_change_made = false;

      rc                     = rewrite_expression(right_expr, right_change_made);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      if (left_change_made || right_change_made) {
        change_made = true;
      }
    } break;

    case ExprType::CONJUNCTION: {
      auto                                      conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());

      vector<unique_ptr<Expression>> &children         = conjunction_expr->children();
      for (unique_ptr<Expression> &child_expr : children) {
        bool sub_change_made = false;

        rc                   = rewrite_expression(child_expr, sub_change_made);
        if (rc != RC::SUCCESS) {

          LOG_WARN("failed to rewriter conjunction sub expression. rc=%s", strrc(rc));
          return rc;
        }

        if (sub_change_made && !change_made) {
          change_made = true;
        }
      }
    } break;

    default: {
      // do nothing
    } break;
  }
  return rc;
}