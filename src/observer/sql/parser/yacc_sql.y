%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/expr/expression.h"
#include "common/type/date_type.h"
#include "sql/parser/parser_common.h"

using namespace std;

/* Suppress a GCC warning that treats freeing stack-allocated arrays in the
  generated Bison parser as an error under -Werror. We push/pop diagnostics
  so other warnings are unaffected. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif

string token_name(const char *sql_string, YYLTYPE *llocp)
{
  return string(sql_string + llocp->first_column, llocp->last_column - llocp->first_column + 1);
}

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, yyscan_t scanner, const char *msg)
{
  unique_ptr<ParsedSqlNode> error_sql_node = make_unique<ParsedSqlNode>(SCF_ERROR);
  error_sql_node->error.error_msg = msg;
  error_sql_node->error.line = llocp->first_line;
  error_sql_node->error.column = llocp->first_column;
  sql_result->add_sql_node(std::move(error_sql_node));
  return 0;
}

ArithmeticExpr *create_arithmetic_expression(ArithmeticExpr::Type type,
                                             Expression *left,
                                             Expression *right,
                                             const char *sql_string,
                                             YYLTYPE *llocp)
{
  ArithmeticExpr *expr = new ArithmeticExpr(type, left, right);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

ComparisonExpr *create_comparison_expression(CompOp comp,
                                             Expression *left,
                                             Expression *right,
                                             const char *sql_string,
                                             YYLTYPE *llocp)
{
  ComparisonExpr *expr = new ComparisonExpr(comp, unique_ptr<Expression>(left), unique_ptr<Expression>(right));
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

Expression *create_and_conjunction(Expression *lhs,
                                   Expression *rhs,
                                   const char *sql_string,
                                   YYLTYPE *llocp)
{
  vector<unique_ptr<Expression>> children;
  children.emplace_back(lhs);
  children.emplace_back(rhs);
  ConjunctionExpr *expr = new ConjunctionExpr(ConjunctionExpr::Type::AND, children);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

UnboundAggregateExpr *create_aggregate_expression(const char *aggregate_name,
                                           Expression *child,
                                           const char *sql_string,
                                           YYLTYPE *llocp)
{
  UnboundAggregateExpr *expr = new UnboundAggregateExpr(aggregate_name, child);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

%}

%define api.pure full
%define parse.error verbose
/* 传递 scanner 与额外上下文(原始 SQL 字符串与输出结果) */
%lex-param   { yyscan_t scanner }
%parse-param { const char *sql_string }
%parse-param { ParsedSqlResult *sql_result }
%parse-param { yyscan_t scanner }

%code requires {
  #include "sql/parser/parse_defs.h"

  /* Forward declarations so the generated header can reference
     YYSTYPE/YYLTYPE and scanner types before their full definitions. */
  union YYSTYPE; typedef union YYSTYPE YYSTYPE;
  struct YYLTYPE; typedef struct YYLTYPE YYLTYPE;
  typedef void *yyscan_t;

  /* Prototypes for scanner functions provided by the reentrant lexer. */
  extern int yylex(YYSTYPE * yylval_param, YYLTYPE * yylloc_param , yyscan_t yyscanner);
  int yylex_init_extra(void *user_defined, yyscan_t *scanner);
  int yylex_destroy(yyscan_t scanner);
  void scan_string(const char *str, yyscan_t scanner);
}

/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *                            sql_node;
  ConditionSqlNode *                         condition;
  Value *                                    value;
  enum CompOp                                comp;
  RelAttrSqlNode *                           rel_attr;
  vector<AttrInfoSqlNode> *                  attr_infos;
  AttrInfoSqlNode *                          attr_info;
  Expression *                               expression;
  vector<unique_ptr<Expression>> *           expression_list;
  vector<Value> *                            value_list;
  vector<ConditionSqlNode> *                 condition_list;
  vector<RelAttrSqlNode> *                   rel_attr_list;
  /* removed relation_list (old rel_list nonterminal) */
  vector<string> *                           key_list;
  JoinRelSqlNode *                           join_rel;  /* INNER JOIN 链 */
  char *                                     cstring;
  int                                        number;
  float                                      floats;
}

//标识tokens
%token  SEMICOLON BY CREATE DROP GROUP TABLE TABLES INDEX CALC SELECT DESC SHOW SYNC INSERT DELETE UPDATE LBRACE RBRACE COMMA TRX_BEGIN TRX_COMMIT TRX_ROLLBACK INT_T STRING_T FLOAT_T VECTOR_T DATE_T INNER JOIN HELP EXIT DOT INTO VALUES FROM WHERE AND LIKE NOT SET ON LOAD DATA INFILE EXPLAIN STORAGE FORMAT PRIMARY KEY ANALYZE FIELDS TERMINATED ENCLOSED EQ LT GT LE GE NE IF_T EXISTS_T

%destructor { delete $$; } <condition>
%destructor { delete $$; } <value>
%destructor { delete $$; } <rel_attr>
%destructor { delete $$; } <attr_infos>
%destructor { delete $$; } <expression>
%destructor { delete $$; } <expression_list>
%destructor { delete $$; } <value_list>
%destructor { delete $$; } <condition_list>
// %destructor { delete $$; } <rel_attr_list>
/* removed destructor for relation_list */
%destructor { delete $$; } <key_list>

%token <number> NUMBER
%token <floats> FLOAT
%token <cstring> ID
%token <cstring> SSS
//非终结符

/** type 定义了各种解析后的结果输出的是什么类型。类型对应了 union 中的定义的成员变量名称 **/
%type <number>              type
%type <condition>           condition
%type <value>               value
%type <number>              number
%type <cstring>             relation
%type <comp>                comp_op
%type <rel_attr>            rel_attr
%type <attr_infos>          attr_def_list
%type <attr_info>           attr_def
%type <value_list>          value_list
%type <condition_list>      condition_list
%type <cstring>             storage_format
%type <key_list>            primary_key
%type <key_list>            attr_list
/* removed rel_list nonterminal */
%type <expression_list>     group_by
%type <expression>          select_item
%type <sql_node>            select_stmt
%type <condition_list>      on_condition_list
%type <condition>           on_condition
%type <join_rel>            join_tail
%type <join_rel>            join_tail_part
%type <expression>          expression
%type <expression>          predicate_expression
%type <expression>          where_select_expr
%type <condition_list>      where
%type <expression>          aggregate_expression
%type <expression_list>     expression_list
%type <cstring>             fields_terminated_by
%type <cstring>             enclosed_by
%type <sql_node>            calc_stmt
%type <sql_node>            insert_stmt
%type <sql_node>            update_stmt
%type <sql_node>            delete_stmt
%type <sql_node>            create_table_stmt
%type <sql_node>            drop_table_stmt
%type <sql_node>            analyze_table_stmt
%type <sql_node>            show_tables_stmt
%type <sql_node>            desc_table_stmt
%type <sql_node>            create_index_stmt
%type <sql_node>            drop_index_stmt
%type <sql_node>            sync_stmt
%type <sql_node>            begin_stmt
%type <sql_node>            commit_stmt
%type <sql_node>            rollback_stmt
%type <sql_node>            load_data_stmt
%type <sql_node>            explain_stmt
%type <sql_node>            set_variable_stmt
%type <sql_node>            help_stmt
%type <sql_node>            exit_stmt
%type <sql_node>            command_wrapper
// commands should be a list but I use a single command instead
%type <sql_node>            commands

%left '+' '-'
%left '*' '/'
%right UMINUS
%%

commands: command_wrapper opt_semicolon  //commands or sqls. parser starts here.
  {
    unique_ptr<ParsedSqlNode> sql_node = unique_ptr<ParsedSqlNode>($1);
    sql_result->add_sql_node(std::move(sql_node));
  }
  ;

command_wrapper:
    calc_stmt
  | select_stmt
  | insert_stmt
  | update_stmt
  | delete_stmt
  | create_table_stmt
  | drop_table_stmt
  | analyze_table_stmt
  | show_tables_stmt
  | desc_table_stmt
  | create_index_stmt
  | drop_index_stmt
  | sync_stmt
  | begin_stmt
  | commit_stmt
  | rollback_stmt
  | load_data_stmt
  | explain_stmt
  | set_variable_stmt
  | help_stmt
  | exit_stmt
    ;

exit_stmt:      
    EXIT {
      (void)yynerrs;  // 这么写为了消除yynerrs未使用的告警。如果你有更好的方法欢迎提PR
      $$ = new ParsedSqlNode(SCF_EXIT);
    };

help_stmt:
    HELP {
      $$ = new ParsedSqlNode(SCF_HELP);
    };

sync_stmt:
    SYNC {
      $$ = new ParsedSqlNode(SCF_SYNC);
    }
    ;

begin_stmt:
    TRX_BEGIN  {
      $$ = new ParsedSqlNode(SCF_BEGIN);
    }
    ;

commit_stmt:
    TRX_COMMIT {
      $$ = new ParsedSqlNode(SCF_COMMIT);
    }
    ;

rollback_stmt:
    TRX_ROLLBACK  {
      $$ = new ParsedSqlNode(SCF_ROLLBACK);
    }
    ;

drop_table_stmt:    /*drop table 语句的语法解析树*/
    DROP TABLE ID {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      $$->drop_table.relation_name = $3;
  }

  | DROP TABLE IF_T EXISTS_T ID
    {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      $$->drop_table.relation_name = $5;
    }

  ;
analyze_table_stmt:  /* analyze table 语法的语法解析树*/
    ANALYZE TABLE ID {
      $$ = new ParsedSqlNode(SCF_ANALYZE_TABLE);
      $$->analyze_table.relation_name = $3;
    }
    ;

show_tables_stmt:
    SHOW TABLES {
      $$ = new ParsedSqlNode(SCF_SHOW_TABLES);
    }
    ;

desc_table_stmt:
    DESC ID  {
      $$ = new ParsedSqlNode(SCF_DESC_TABLE);
      $$->desc_table.relation_name = $2;
    }
    ;

create_index_stmt:    /*create index 语句的语法解析树*/
    CREATE INDEX ID ON ID LBRACE attr_list RBRACE
    {
      $$ = new ParsedSqlNode(SCF_CREATE_INDEX);
      CreateIndexSqlNode &create_index = $$->create_index;
      create_index.index_name = $3;
      create_index.relation_name = $5;
      if ($7 != nullptr) { create_index.attribute_names.swap(*$7); delete $7; }
    }
    ;

drop_index_stmt:      /*drop index 语句的语法解析树*/
    DROP INDEX ID ON ID
    {
      $$ = new ParsedSqlNode(SCF_DROP_INDEX);
      $$->drop_index.index_name = $3;
      $$->drop_index.relation_name = $5;
    }
    ;
create_table_stmt:    /*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def_list primary_key RBRACE storage_format
    {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;
      create_table.attr_infos.swap(*$5);
      delete $5;
      if ($6 != nullptr) {
        create_table.primary_keys.swap(*$6);
        delete $6;
      }
      if ($8 != nullptr) {
        create_table.storage_format = $8;
      }
    }
    | CREATE TABLE IF_T NOT EXISTS_T ID LBRACE attr_def_list primary_key RBRACE storage_format
    {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      /* 支持 IF NOT EXISTS */
      create_table.relation_name = $6;            /* ID */
      create_table.attr_infos.swap(*$8);          /* attr_def_list */
      delete $8;
      if ($9 != nullptr) {                        /* primary_key */
        create_table.primary_keys.swap(*$9);
        delete $9;
      }
      if ($11 != nullptr) {                       /* storage_format */
        create_table.storage_format = $11;
      }
  create_table.if_not_exists = true;
    }
    ;

attr_def_list:
    attr_def
    {
      $$ = new vector<AttrInfoSqlNode>;
      $$->emplace_back(*$1);
      delete $1;
    }
    | attr_def_list COMMA attr_def
    {
      $$ = $1;
      $$->emplace_back(*$3);
      delete $3;
    }
    ;
    
attr_def:
    ID type LBRACE number RBRACE 
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = $4;
    }
    | ID type
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
    }
    ;
number:
    NUMBER {$$ = $1;}
    ;
type:
    INT_T      { $$ = static_cast<int>(AttrType::INTS); }
    | STRING_T { $$ = static_cast<int>(AttrType::CHARS); }
    | FLOAT_T  { $$ = static_cast<int>(AttrType::FLOATS); }
    | VECTOR_T { $$ = static_cast<int>(AttrType::VECTORS); }
  | DATE_T   { $$ = static_cast<int>(AttrType::DATES); }
    ;
primary_key:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA PRIMARY KEY LBRACE attr_list RBRACE
    {
      $$ = $5;
    }
    ;

attr_list:
    ID {
      $$ = new vector<string>();
      $$->push_back($1);
    }
    | ID COMMA attr_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new vector<string>;
      }

      $$->insert($$->begin(), $1);
    }
    ;

insert_stmt:        /*insert   语句的语法解析树*/
    INSERT INTO ID VALUES LBRACE value_list RBRACE 
    {
      $$ = new ParsedSqlNode(SCF_INSERT);
      $$->insertion.relation_name = $3;
      $$->insertion.values.swap(*$6);
      delete $6;
    }
    ;

value_list:
    value
    {
      $$ = new vector<Value>;
      $$->emplace_back(*$1);
      delete $1;
    }
    | value_list COMMA value { 
      $$ = $1;
      $$->emplace_back(*$3);
      delete $3;
    }
    ;
value:
    NUMBER {
      $$ = new Value((int)$1);
      @$ = @1;
    }
    |FLOAT {
      $$ = new Value((float)$1);
      @$ = @1;
    }
    |SSS {
      char *tmp = common::substr($1,1,strlen($1)-2);
      int days = 0; string date_str(tmp);
      if (OB_SUCC(DateType::parse(date_str, days))) {
        $$ = new Value();
        $$->set_type(AttrType::DATES);
        $$->set_int(days); // set_int 会把类型改成 INTS，后面再恢复
        $$->set_type(AttrType::DATES);
      } else {
        $$ = new Value(tmp);
      }
      free(tmp);
    }
    ;
storage_format:
    /* empty */
    {
      $$ = nullptr;
    }
    | STORAGE FORMAT EQ ID
    {
      $$ = $4;
    }
    ;
    
delete_stmt:    /*  delete 语句的语法解析树*/
    DELETE FROM ID where 
    {
      $$ = new ParsedSqlNode(SCF_DELETE);
      $$->deletion.relation_name = $3;
      if ($4 != nullptr) {
        $$->deletion.conditions.swap(*$4);
        delete $4;
      }
    }
    ;
update_stmt:      /*  update 语句的语法解析树*/
    UPDATE ID SET ID EQ value where 
    {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      $$->update.relation_name = $2;
      $$->update.attribute_name = $4;
      $$->update.value = *$6;
      if ($7 != nullptr) {
        $$->update.conditions.swap(*$7);
        delete $7;
      }
    }
    ;
/* 重写 select 支持:
 * SELECT expr_list FROM base_table { INNER JOIN table ON cond_list }*
 *   [ WHERE condition_list | WHERE predicate_expression ]
 *   group_by
 */
select_stmt:
  SELECT expression_list FROM relation join_tail where_select_expr group_by
    {
      $$ = new ParsedSqlNode(SCF_SELECT);
      if ($2 != nullptr) { $$->selection.expressions.swap(*$2); delete $2; }
      $$->selection.relations.push_back($4);
      if ($5 != nullptr) {
        for (auto &rel : $5->relations) {
          $$->selection.relations.push_back(rel);
        }
        $$->selection.conditions.insert($$->selection.conditions.end(), $5->conditions.begin(), $5->conditions.end());
        delete $5;
      }
      if ($6 != nullptr) {
        $$->selection.where_expr.reset($6);
      }
      if ($7 != nullptr) { $$->selection.group_by.swap(*$7); delete $7; }
    }
    ;

where_select_expr:
    /* empty */ { $$ = nullptr; }
  | WHERE predicate_expression { $$ = $2; }
  ;

join_tail:
    /* empty */ { $$ = nullptr; }
  | join_tail join_tail_part {
      if ($1 == nullptr) { $$ = $2; }
      else {
        for (auto &rel : $2->relations) { $1->relations.push_back(rel); }
        for (auto &c : $2->conditions) { $1->conditions.push_back(c); }
        delete $2; $$ = $1;
      }
    }
  ;

join_tail_part:
    INNER JOIN relation ON on_condition_list {
      $$ = new JoinRelSqlNode();
      $$->relations.push_back($3);
      if ($5 != nullptr) {
        for (auto &cond : *$5) { $$->conditions.push_back(cond); }
        delete $5;
      }
    }
    ;

on_condition_list:
    on_condition {
      $$ = new vector<ConditionSqlNode>();
      $$->emplace_back(*$1);
      delete $1;
    }
    | on_condition AND on_condition_list {
      $$ = $3;
      $$->emplace_back(*$1);
      delete $1;
    }
    ;

on_condition:
    condition { $$ = $1; }
    ;

/* join_tail 可以为空或多个 JOIN */
/* join_tail 语义: { INNER JOIN relation ON on_condition_list }* */

calc_stmt:
    CALC expression_list
    {
      $$ = new ParsedSqlNode(SCF_CALC);
      $$->calc.expressions.swap(*$2);
      delete $2;
    }
    ;

/*
 * 统一 WHERE 子句（表达式）：
 * - SELECT: 走这里，得到 where_expr（Expression*）
 * - DELETE/UPDATE: 仍走旧 where(condition_list) 非终结符（见下方 where:）
 */
// NOTE: select 只使用 where_select_expr (WHERE predicate_expression)，避免旧 condition_list 产生式抢占。

predicate_expression:
    expression comp_op expression {
      $$ = create_comparison_expression($2, $1, $3, sql_string, &@$);
    }
  | predicate_expression AND predicate_expression {
      $$ = create_and_conjunction($1, $3, sql_string, &@$);
    }
  | LBRACE predicate_expression RBRACE {
      $$ = $2;
      $$->set_name(token_name(sql_string, &@$));
    }
  ;

expression_list:
    select_item
    {
      $$ = new vector<unique_ptr<Expression>>;
      $$->emplace_back($1);
    }
    | select_item COMMA expression_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new vector<unique_ptr<Expression>>;
      }
      $$->emplace($$->begin(), $1);
    }
    ;

select_item:
    expression { $$ = $1; }
  ;
expression:
    expression '+' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::ADD, $1, $3, sql_string, &@$);
    }
  | expression '-' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::SUB, $1, $3, sql_string, &@$);
    }
  | expression '*' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::MUL, $1, $3, sql_string, &@$);
    }
  | expression '/' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::DIV, $1, $3, sql_string, &@$);
    }
  | LBRACE expression RBRACE {
      $$ = $2;
      $$->set_name(token_name(sql_string, &@$));
    }
  | '-' expression %prec UMINUS {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::NEGATIVE, $2, nullptr, sql_string, &@$);
    }
  | '*' {
      $$ = new StarExpr();
    }
  | value {
      $$ = new ValueExpr(*$1);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
  | rel_attr {
      RelAttrSqlNode *node = $1;
      $$ = new UnboundFieldExpr(node->relation_name, node->attribute_name);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
  | aggregate_expression {
      $$ = $1;
    }
    ;

aggregate_expression:
    ID LBRACE expression RBRACE {
      $$ = create_aggregate_expression($1, $3, sql_string, &@$);
    }
    ;

rel_attr:
    ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
    }
    ;

relation:
    ID {
      $$ = $1;
    }
    ;
/* rel_list removed: multi-table selects now handled via relation + join_tail */

where:
    /* empty */
    {
      $$ = nullptr;
    }
    | WHERE condition_list {
      $$ = $2;
    }
    ;

condition_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | condition {
      $$ = new vector<ConditionSqlNode>;
      $$->emplace_back(*$1);
      delete $1;
    }
    | condition AND condition_list {
      $$ = $3;
      $$->emplace_back(*$1);
      delete $1;
    }
    ;
condition:
    rel_attr comp_op value
    {
      $$ = new ConditionSqlNode;
      $$->left_is_attr = 1;
      $$->left_attr = *$1;
      $$->right_is_attr = 0;
      $$->right_value = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | value comp_op value 
    {
      $$ = new ConditionSqlNode;
      $$->left_is_attr = 0;
      $$->left_value = *$1;
      $$->right_is_attr = 0;
      $$->right_value = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | rel_attr comp_op rel_attr
    {
      $$ = new ConditionSqlNode;
      $$->left_is_attr = 1;
      $$->left_attr = *$1;
      $$->right_is_attr = 1;
      $$->right_attr = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | value comp_op rel_attr
    {
      $$ = new ConditionSqlNode;
      $$->left_is_attr = 0;
      $$->left_value = *$1;
      $$->right_is_attr = 1;
      $$->right_attr = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    ;

comp_op:
      EQ { $$ = EQUAL_TO; }
    | LT { $$ = LESS_THAN; }
    | GT { $$ = GREAT_THAN; }
    | LE { $$ = LESS_EQUAL; }
    | GE { $$ = GREAT_EQUAL; }
    | NE { $$ = NOT_EQUAL; }
  | LIKE { $$ = LIKE_OP; }
  | NOT LIKE { $$ = NOT_LIKE_OP; }
    ;

// your code here
group_by:
    /* empty */
    {
      $$ = nullptr;
    }
    | GROUP BY expression_list
    {
      // group by 的表达式范围与select查询值的表达式范围是不同的，比如group by不支持 *
      // 但是这里没有处理。
      $$ = $3;
    }
    ;
load_data_stmt:
    LOAD DATA INFILE SSS INTO TABLE ID fields_terminated_by enclosed_by
    {
      char *tmp_file_name = common::substr($4, 1, strlen($4) - 2);
      
      $$ = new ParsedSqlNode(SCF_LOAD_DATA);
      $$->load_data.relation_name = $7;
      $$->load_data.file_name = tmp_file_name;
      if ($8 != nullptr) {
        char *tmp = common::substr($8,1,strlen($8)-2);
        $$->load_data.terminated = $8;
        free(tmp);
      }
      if ($9 != nullptr) {
        char *tmp = common::substr($9,1,strlen($9)-2);
        $$->load_data.enclosed = $9;
        free(tmp);
      }
      free(tmp_file_name);
    }
    ;

fields_terminated_by:
    /* empty */
    {
      $$ = nullptr;
    }
    | FIELDS TERMINATED BY SSS
    {
      $$ = $4;
    };

enclosed_by:
    /* empty */
    {
      $$ = nullptr;
    }
    | ENCLOSED BY SSS
    {
      $$ = $3;
    };

explain_stmt:
    EXPLAIN command_wrapper
    {
      $$ = new ParsedSqlNode(SCF_EXPLAIN);
      $$->explain.sql_node = unique_ptr<ParsedSqlNode>($2);
    }
    ;

set_variable_stmt:
    SET ID EQ value
    {
      $$ = new ParsedSqlNode(SCF_SET_VARIABLE);
      $$->set_variable.name  = $2;
      $$->set_variable.value = *$4;
      delete $4;
    }
    ;

opt_semicolon: /*empty*/
    | SEMICOLON
    ;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, ParsedSqlResult *sql_result) {
  yyscan_t scanner;
  std::vector<char *> allocated_strings;
  yylex_init_extra(static_cast<void*>(&allocated_strings),&scanner);
  scan_string(s, scanner);
  /* 新的 yyparse 只需要传入我们在 %parse-param 中声明的参数 */
  int result = yyparse(s, sql_result, scanner);

  for (char *ptr : allocated_strings) {
    free(ptr);
  }
  allocated_strings.clear();

  yylex_destroy(scanner);
  return result;
}