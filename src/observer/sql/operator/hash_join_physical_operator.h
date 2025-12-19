/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "sql/expr/expression.h"
#include <unordered_map>

/**
 * @brief 简单等值 Hash Join 算子（仅支持单个等值字段对，后续可扩展多字段）
 */
class HashJoinPhysicalOperator : public PhysicalOperator
{
public:
    HashJoinPhysicalOperator(unique_ptr<Expression> left_key, unique_ptr<Expression> right_key,
                                                     vector<unique_ptr<Expression>> &&other_preds);
    virtual ~HashJoinPhysicalOperator() = default;

    PhysicalOperatorType type() const override { return PhysicalOperatorType::HASH_JOIN; }
    OpType get_op_type() const override { return OpType::INNERHASHJOIN; }

    RC open(Trx *trx) override;
    RC next() override;
    RC close() override;
    Tuple *current_tuple() override { return &joined_tuple_; }

private:
    struct BucketRow { ValueListTuple tuple; };
    using Bucket = vector<BucketRow>;
    unordered_multimap<int, BucketRow> int_map_; // 简化仅支持 INT/FLOAT 哈希（INT key）
    unordered_multimap<string, BucketRow> str_map_;
    bool use_int_key_ = true;

    unique_ptr<Expression> left_key_expr_;  // build side key
    unique_ptr<Expression> right_key_expr_; // probe side key
    vector<unique_ptr<Expression>> other_preds_; // 其他 join_predicates_ 除了主等值

    // probe 状态
    vector<BucketRow> probe_matches_;
    size_t probe_pos_ = 0;
    Tuple *probe_right_tuple_ = nullptr;
    JoinedTuple joined_tuple_;
    Trx *trx_ = nullptr;
    PhysicalOperator *left_child_ = nullptr;
    PhysicalOperator *right_child_ = nullptr;
    RC build_phase();
    bool predicate_ok(const Tuple &left, const Tuple &right);
};