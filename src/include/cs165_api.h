/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>

#include "common.h"

#define DEFAULT_NUM_TABLES 50
#define DEFAULT_NUM_COLS 500
#define DEFAULT_NUM_VALS 5000000
#define DEFAULT_VAR_NAME_LENGTH 10
#define DEFAULT_NUM_CLIENTS_ALLOWED 10
#define DEFAULT_CATALOG_RESULTS 2000
#define DEFAULT_SHARED_SCAN_BUFFER_SIZE 10

#define HASH_THRESHOLD 4096
#define PAGESIZE 524288
#define CACHESIZE 24

// Set bool type
#define bool char
#define false 0
#define true 1

/**
 * IndexType
 * Flag to identify index type. Defines an enum for the different possible column indices.
 * Additonal types are encouraged as extra.
 **/
typedef enum IndexType {
    NONE,
    SORTED,
    B_PLUS_TREE,
} IndexType;

/**
 * column_index
 * Defines a general column_index structure, which can be used as a sorted
 * index or a b+-tree index.
 * - type, the column index type (see enum index_type)
 * - index, a pointer to the index structure. For SORTED, this points to the
 *       start of the sorted array. For B+Tree, this points to the root node.
 *       You will need to cast this from void* to the appropriate type when
 *       working with the index.
 **/
typedef struct column_index {
    IndexType type;
    void* index;
} column_index;

/**
 * column
 * Defines a column structure, which is the building block of our column-store.
 * Operations can be performed on one or more columns.
 * - name, the string associated with the column. Column names must be unique
 *       within a table, but columns from different tables can have the same
 *       name.
 * - data, this is the raw data for the column. Operations on the data should
 *       be persistent.
 * - index, this is an [opt] index built on top of the column's data.
 *
 * NOTE: We do not track the column length in the column struct since all
 * columns in a table should share the same length. Instead, this is
 * tracked in the table (length).
 **/
typedef struct column {
    const char* name;
    int* data;
    column_index *index;
    size_t data_count;
    bool leading;
} column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * in clude a size_t table_size).
 * name, the name associated with the table. Table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - col, this is the pointer to an array of columns contained in the table.
 * - length, the size of the columns in the table.
 **/
typedef struct table {
    const char* name;
    size_t col_count;
    column** col;
    int leading_idx;
} table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - table_count: the number of tables in the database.
 * - tables: the pointer to the array of tables contained in the db.
 **/
typedef struct db {
    const char* name;
    size_t table_count;
    table** tables;
} db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call.
  */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct status {
    StatusCode code;
    char* error_message;
} status;

// typedef struct row {
//     void* vals;
// } row;

typedef struct result {
    size_t num_tuples;
    void *payload;
    DataType type;
    size_t max_size;
} result;

typedef enum Aggr {
    MIN,
    MAX,
    AVG,
    CNT,
} Aggr;

typedef enum OperatorType {
    CREATE_OP,
    SELECT,
    PROJECT,
    JOIN,
    INSERT,
    DELETE,
    UPDATE,
    AGGREGATE,
    TUPLE,
    SHUTDOWN,
    ADD,
    SUB,
    SHARED_SCAN,
} OperatorType;

typedef struct tuples {
    void** payloads;
    size_t num_rows;
    size_t num_cols;
    DataType type;
} tuples;

/**
 * db_operator
 * The db_operator defines a database operation.  Generally, an operation will
 * be applied on column(s) of a table (SELECT, PROJECT, AGGREGATE) while other
 * operations may involve multiple tables (JOINS). The OperatorType defines
 * the kind of operation.
 *
 * In UNARY operators that only require a single table, only the variables
 * related to table1/column1 will be used.
 * In BINARY operators, the variables related to table2/column2 will be used.
 *
 * If you are operating on more than two tables, you should rely on separating
 * it into multiple operations (e.g., if you have to project on more than 2
 * tables, you should select in one operation, and then create separate
 * operators for each projection).
 *
 * Example query:
 * SELECT a FROM A WHERE A.a < 100;
 * db_operator op1;
 * op1.table1 = A;
 * op1.column1 = b;
 *
 * filter f;
 * f.value = 100;
 * f.type = LESS_THAN;
 * f.mode = NONE;
 *
 * op1.comparator = f;
 **/
typedef struct db_operator {
    // Flag to choose operator
    OperatorType type;

    // Used for every operator
    table** tables;
    column** columns;

    // Variable names of intermediate result
    char* name1;
    char* name2;

    // For insert/delete operations, we only use value1;
    // For update operations, we update value1 -> value2;
    int *value1;
    int *value2;

    // Pointer to results for commands that require intermediate results
    result* result1;
    result* result2;
    result* result3;
    result* result4;

    // Row struct to hold tuples
    tuples* tups;

    // This includes several possible fields that may be used in the operation.
    Aggr agg;

    // comparators
    int lower;
    int upper;

} db_operator;

typedef enum OpenFlags {
    CREATE = 1,
    LOAD = 2,
} OpenFlags;

typedef struct catalog {
    char* names[DEFAULT_CATALOG_RESULTS];
    result* results[DEFAULT_CATALOG_RESULTS];
    size_t var_count;
} catalog;

typedef struct thread_args {
    db_operator* query;
    result* res;
    size_t start;
    size_t end;
} thread_args;

typedef struct select_queue {
    thread_args** buffer;
    size_t buffer_count;
    column* col;
} select_queue;

/* OPERATOR API*/
/**
 * open_db(filename, db, flags)
 * Opens the file associated with @filename and loads its contents into @db.
 *
 * If flags | Create, then it should create a new db.
 * If flags | Load, then it should load the db with the incoming data.
 *
 * Note that the database in @filename MUST contain the same name as db->name
 * (if db != NULL). If not, then return an error.
 *
 * filename: the name associated with the DB file
 * db      : the pointer to db*
 * flags   : the flags indicating the create/load options
 * returns : a status of the operation.
 */
status open_db(const char* filename, db** db, OpenFlags flags);

/**
 * drop_db(db)
 * Drops the database associated with db.  You should permanently delete
 * the db and all of its tables/columns.
 *
 * db       : the database to be dropped.
 * returns  : the status of the operation.
 **/
status drop_db(db* db);

/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
status sync_db(db* db);

/**
 * create_db(db_name, db)
 * Creates a database with the given database name, and stores the pointer in db
 *
 * db_name  : name of the database, must be unique.
 * db       : pointer to the db pointer. If *db == NULL, then create_db is
 *            responsible for allocating space for the db, else it should assume
 *            that *db points to pre-allocated space.
 * returns  : the status of the operation.
 *
 * Usage:
 *  db *database = NULL;
 *  status s = create_db("db_cs165", &database)
 *  if (s.code != OK) {
 *      // Something went wrong
 *  }
 **/
status create_db(const char* db_name, db** db);

/**
 * create_table(db, name, num_columns, table)
 * Creates a table named @name in @db with @num_columns, and stores the pointer
 * in @table.
 *
 * db          : the database in which to create the table.
 * name        : the name of the new table, must be unique in the db.
 * num_columns : the non-negative number of columns in the table.
 * table       : the pointer to the table pointer. If *table == NULL, then
 *               create_table is responsible for allocating space for a table,
 *               else it assume that *table points to pre-allocated space.
 * returns     : the status of the operation.
 *
 * Usage:
 *  // Assume you have a valid db* 'database'
 *  table* tbl = NULL;
 *  status s = create_table(database, "tbl_cs165", 4, &tbl)
 *  if (s.code != OK) {
 *      // Something went wrong
 *  }
 **/
status create_table(db* db, const char* name, size_t num_columns, table** table);

/**
 * drop_table(db, table)
 * Drops the table from the db.  You should permanently delete
 * the table and all of its columns.
 *
 * db       : the database that contains the table.
 * table    : the table to be dropped.
 * returns  : the status of the operation.
 **/
status drop_table(db* db, table* table);

/**
 * create_column(table, name, col)
 * Creates a column named @name in @table, and stores the pointer in @col.
 *
 * table   : the table in which to create the column.
 * name    : the name of the column, must be unique in the table.
 * col     : the pointer to the column pointer. If *col == NULL, then
 *           create_column is responsible for allocating space for a column*,
 *           else it should assume that *col points to pre-allocated space.
 * returns : the status of the operation.
 *
 * Usage:
 *  // Assume that you have a valid table* 'tbl';
 *  column* col;
 *  status s = create_column(tbl, 'col_cs165', &col)
 *  if (s.code != OK) {
 *      // Something went wrong
 *  }
 **/
status create_column(table *table, const char* name, column** col, bool sorted);

/**
 * create_index(col, type)
 * Creates an index for @col of the given IndexType. It stores the created index
 * in col->index.
 *
 * col      : the column for which to create the index.
 * type     : the enum representing the index type to be created.
 * returns  : the status of the operation.
 **/
status create_index(column* col, IndexType type);

status col_insert(column *col, int data);
status delete(column *col, int *pos);
status update(column *col, int *pos, int new_val);
status select_data(db_operator* query, result **r);
status index_scan(int lower, int upper, column *col, result **r);
status col_scan(int lower, int upper, column *col, result **r);
status vec_scan(db_operator* query, result** r);
status fetch(column *col, int* indices, size_t val_count, result **r);

status add_col(int* vals1, int* vals2, size_t num_vals, result** r);
status sub_col(int* vals1, int* vals2, size_t num_vals, result** r);
status max_col(result* inter, result** r);
status min_col(result* inter, result** r);
status avg_col(result* inter, result** r);
status count_col(size_t num_vals, result** r);
status process_indexes(table* tbl);


/* Query API */
status query_prepare(const char* query, db_operator** op);
status query_execute(db_operator* op, result** results);


#endif /* CS165_H */

