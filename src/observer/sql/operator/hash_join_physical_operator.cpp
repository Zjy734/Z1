/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/hash_join_physical_operator.h"
#include "sql/expr/tuple.h"
#include "common/log/log.h"

HashJoinPhysicalOperator::HashJoinPhysicalOperator(unique_ptr<Expression> left_key, unique_ptr<Expression> right_key,
                                                                                                     vector<unique_ptr<Expression>> &&other_preds)
    : left_key_expr_(std::move(left_key)), right_key_expr_(std::move(right_key)), other_preds_(std::move(other_preds)) {}

RC HashJoinPhysicalOperator::open(Trx *trx) {
    trx_ = trx;
    if (children_.size() != 2) {
        LOG_WARN("hash join requires 2 children");
        return RC::INTERNAL;
    }
    left_child_ = children_[0].get();
    right_child_ = children_[1].get();
    RC rc = left_child_->open(trx);
    if (rc != RC::SUCCESS) return rc;
    rc = build_phase();
    if (rc != RC::SUCCESS) return rc;
    rc = right_child_->open(trx);
    return rc;
}

RC HashJoinPhysicalOperator::build_phase() {
    RC rc = RC::SUCCESS;
    while (RC::SUCCESS == (rc = left_child_->next())) {
        Tuple *lt = left_child_->current_tuple();
        Value key_v; rc = left_key_expr_->get_value(*lt, key_v); if (rc != RC::SUCCESS) return rc;
        BucketRow br; ValueListTuple vl; ValueListTuple::make(*lt, vl); br.tuple = vl;
        if (key_v.attr_type() == AttrType::INTS || key_v.attr_type() == AttrType::FLOATS) {
            use_int_key_ = true; int_map_.emplace(key_v.get_int(), std::move(br));
        } else if (key_v.attr_type() == AttrType::CHARS) {
            use_int_key_ = false; str_map_.emplace(key_v.get_string(), std::move(br));
        } else {
            LOG_WARN("unsupported join key type %d", key_v.attr_type());
            return RC::UNIMPLEMENTED;
        }
    }
    if (rc == RC::RECORD_EOF) rc = RC::SUCCESS;
    return rc;
}

bool HashJoinPhysicalOperator::predicate_ok(const Tuple &left, const Tuple &right) {
    // 目前 other_preds_ 已在逻辑层包装为单独 predicate（物理生成时），此处留扩展点
    (void)left; (void)right; return true;
}

RC HashJoinPhysicalOperator::next() {
    RC rc = RC::SUCCESS;
    while (true) {
        if (probe_pos_ < probe_matches_.size()) {
            // 已设置 joined_tuple_ 内容
            ValueListTuple &left_tuple = probe_matches_[probe_pos_].tuple;
            joined_tuple_.set_left(&left_tuple);
            joined_tuple_.set_right(probe_right_tuple_);
            ++probe_pos_;
            return RC::SUCCESS;
        }
        rc = right_child_->next();
        if (rc != RC::SUCCESS) return rc; // EOF or error
        probe_right_tuple_ = right_child_->current_tuple();
        Value rkey; rc = right_key_expr_->get_value(*probe_right_tuple_, rkey); if (rc != RC::SUCCESS) return rc;
        probe_matches_.clear(); probe_pos_ = 0;
        if (use_int_key_) {
            int k = (rkey.attr_type()==AttrType::INTS)? rkey.get_int() : (int)rkey.get_float();
            auto range = int_map_.equal_range(k);
            for (auto it = range.first; it != range.second; ++it) probe_matches_.push_back(it->second);
        } else {
            auto range = str_map_.equal_range(rkey.get_string());
            for (auto it = range.first; it != range.second; ++it) probe_matches_.push_back(it->second);
        }
        if (probe_matches_.empty()) continue; // 无匹配继续 probe
        // 准备第一条匹配
        ValueListTuple &left_tuple = probe_matches_[0].tuple;
        joined_tuple_.set_left(&left_tuple);
        joined_tuple_.set_right(probe_right_tuple_);
        probe_pos_ = 1;
        return RC::SUCCESS;
    }
}

RC HashJoinPhysicalOperator::close() {
    RC rc = RC::SUCCESS;
    if (left_child_) { RC rc2 = left_child_->close(); if (rc == RC::SUCCESS) rc = rc2; }
    if (right_child_) { RC rc2 = right_child_->close(); if (rc == RC::SUCCESS) rc = rc2; }
    int_map_.clear(); str_map_.clear(); probe_matches_.clear();
    return rc;
}