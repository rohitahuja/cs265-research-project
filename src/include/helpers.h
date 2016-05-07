#ifndef HELPERS_H__
#define HELPERS_H__

#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include "cs165_api.h"
#include "shared_scan.h"

// PARSER FUNCTIONS
int find_table(char* tbl_name);
int find_column(table* tbl, char* col_name);
int find_table_from_col_name(char* col_name);
int create_lower_bound(char* val);
int create_upper_bound(char* val);
result* find_result(char* name);
status prepare_result(char* var_name, result** r);
status relational_insert(int tbl_idx, char* vals, db_operator* op);

// SERVER FUNCTIONS
catalog** init_catalogs();
status persist_data();
status grab_persisted_data();
void free_result(result* res);
status free_catalogs();
db_operator* init_dbo();
tuples* init_tuples();

// INDEX FUNCTIONS
int binary_search(int* data, int target, int start, int end);

// SELECT FUNCTIONS
int check_data(int data, int lower, int upper);


#endif // HELPERS_H__