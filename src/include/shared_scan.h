#ifndef SHARED_SCAN_H__
#define SHARED_SCAN_H__

#include <stdio.h>
#include <pthread.h>
#include "cs165_api.h"
#include "helpers.h"
#include "utils.h"

extern catalog** catalogs;
extern bool sscan;

status shared_scan_select(column* col, select_queue** queue);
select_queue* init_select_queue();
status flush_queue(select_queue** queue, char* name);

#endif // SHARED_SCAN_H__