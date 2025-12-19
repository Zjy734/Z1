#include "sql/executor/drop_table_executor.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "session/session.h"
#include "sql/stmt/drop_table_stmt.h"
#include "sql/executor/sql_result.h"
#include "storage/db/db.h"

RC DropTableExecutor::execute(SQLStageEvent *sql_event)
{
  auto *stmt = sql_event->stmt();
  auto *session = sql_event->session_event()->session();

  if (stmt->type() != StmtType::DROP_TABLE) {
    return RC::INVALID_ARGUMENT;
  }

  auto *drop_stmt = static_cast<DropTableStmt *>(stmt);
  const char *table_name = drop_stmt->table_name().c_str();

  RC rc = session->get_current_db()->drop_table(table_name);
  sql_event->session_event()->sql_result()->set_return_code(rc);
  return rc;
}