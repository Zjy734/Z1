// Drop table executor
#pragma once

#include "common/sys/rc.h"

class SQLStageEvent;

class DropTableExecutor {
public:
  RC execute(SQLStageEvent *sql_event);
};