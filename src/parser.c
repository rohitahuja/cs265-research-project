#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>

#include "db.h"
#include "utils.h"
#include "helpers.h"


// This tells the linker that there exists a global_db and catalog external
// from this file.
extern db* global_db;
extern catalog** catalogs;
extern select_queue* queue;

// Prototype for Helper function that executes that actual parsing after
// parse_command_string has found a matching regex.
status parse_dsl(char* str, dsl* d, db_operator* op);

// Finds a possible matching DSL command by using regular expressions.
// If it finds a match, it calls parse_command to actually process the dsl.
status parse_command_string(char* str, dsl** commands, db_operator* op)
{
    log_info("Parsing: %s", str);

    // Create a regular expression to parse the string
    regex_t regex;
    int ret;

    // Track the number of matches; a string must match all
    int n_matches = 1;
    regmatch_t m;

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        dsl* d = commands[i];
        if (regcomp(&regex, d->c, REG_EXTENDED) != 0) {
            log_err("Could not compile regex\n");
        }

        // Bind regular expression associated with the string
        ret = regexec(&regex, str, n_matches, &m, 0);
        // If we have a match, then figure out which one it is!
        if (ret == 0) {
            log_info("Found Command: %d\n", i);
            // Here, we actually strip the command as appropriately
            // based on the DSL to get the variable names.
            return parse_dsl(str, d, op);
        }
    }

    // Nothing was found!
    status s;
    s.code = ERROR;
    s.error_message = "Cannot find match\n";
    log_err(s.error_message);
    return s;
}

status parse_dsl(char* str, dsl* d, db_operator* op) {
    // Use the commas to parse out the string
    char open_paren[2] = "(";
    char close_paren[2] = ")";
    char comma[2] = ",";
    char quotes[2] = "\"";
    // char end_line[2] = "\n";
    // char eq_sign[2] = "=";

    if (d->g == CREATE_DB) {
        // Declare status
        status s;

        // Check if DB exists
        if (global_db) {
            s.code = ERROR;
            s.error_message = "DB already exists\n";
            log_err(s.error_message);
            return s;
        }

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str));
        strncpy(str_cpy, str, strlen(str));

        // This gives us everything inside the (db, "<db_name>")
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "db", but we don't need to use it
        char* db_indicator = strtok(args, comma);
        (void) db_indicator;

        // This gives us "<db_name>"
        char* db_name = strtok(NULL, quotes);

        log_info("create_db(%s)\n", db_name);

        // Here, we can create the DB using our parsed info!
        db* db1 = NULL;
        s = create_db(db_name, &db1);

        // Free the str_cpy
        // free(str_cpy);

        if (s.code != OK) {
            // Something went wrong
            log_err(s.error_message);
            return s;
        }

        // Set global DB variable
        global_db = db1;

        op->type = CREATE_OP;
        return s;
    } else if (d->g == CREATE_TABLE) {
        status s;
        
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (table, <tbl_name>, <db_name>, <count>)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "table"
        char* tbl_indicator = strtok(args, comma);
        (void) tbl_indicator;

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, quotes);

        // This gives us <db_name>, we will need this to create the full name
        char* db_name = strtok(NULL, comma);

        // Generate the full name using <db_name>.<tbl_name>
        size_t name_len = strlen(tbl_name) + strlen(db_name) + 2;
        char* full_name = (char*)malloc(sizeof(char)*(name_len));
        memset(full_name, '\0', name_len); 

        strncat(full_name, db_name, strlen(db_name));
        strncat(full_name, ".", 1);
        strncat(full_name, tbl_name, strlen(tbl_name));
        
        // This gives us count
        char* count_str = strtok(NULL, comma);
        int count = 0;
        if (count_str != NULL) {
            count = atoi(count_str);
        }
        (void) count;

        log_info("create_table(%s, %s, %d)\n", full_name, db_name, count);
        
        // Check if table already exists
        int tbl_idx = find_table(full_name);
        if (tbl_idx != -1) {
            s.code = ERROR;
            s.error_message = "Table name already exists\n";
            log_err(s.error_message);
            return s;
        }

        // Here, we can create the table using our parsed info!
        table* tbl1 = NULL;
        s = create_table(global_db, full_name, count, &tbl1);

        // Free the str_cpy
        // free(str_cpy);
        // free(full_name);
        str_cpy=NULL;
        full_name=NULL;

        if (s.code != OK) {
            // Something went wrong
            log_err(s.error_message);
            return s;
        }

        op->type = CREATE_OP;
        return s;
    } else if (d->g == CREATE_COLUMN) {
        status s;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (col, <col_name>, <tbl_name>, unsorted)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "col"
        char* col_indicator = strtok(args, comma);
        (void) col_indicator;

        // This gives us <col_name>, we will need this to create the full name
        char* col_name = strtok(NULL, quotes);

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, comma);
        
        // Generate the full name using <db_name>.<tbl_name>
        size_t name_len = strlen(tbl_name) + strlen(col_name) + 2;
        char* full_name=(char*)malloc(sizeof(char)*(name_len));
        memset(full_name, '\0', name_len); 

        strncat(full_name, tbl_name, strlen(tbl_name));
        strncat(full_name, ".", 1);
        strncat(full_name, col_name, strlen(col_name));

        // This gives us the "unsorted"
        char* sorting_str = strtok(NULL, comma); 
        bool sorted = false;
        if (strcmp(sorting_str, "sorted") == 0) {
            sorted = true;
        }

        log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);

        // Check that table already exists
        int tbl_idx = find_table(tbl_name);
        if (tbl_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find table\n";
            log_err(s.error_message);
            return s;
        }
        table* table1 = global_db->tables[tbl_idx];

        // Check that column doesn't already exist
        int col_idx = find_column(table1, full_name);
        if (col_idx != -1) {
            s.code = ERROR;
            s.error_message = "Column name already exists\n";
            log_err(s.error_message);
            return s;
        }

        // Here, we can create the column using our parsed info!
        column* col1 = NULL;
        s = create_column(table1, full_name, &col1, sorted);

        // Free the str_cpy
        // free(str_cpy);
        // free(full_name);
        str_cpy=NULL;
        full_name=NULL;

        if (s.code != OK) {
            // Something went wrong
            log_err(s.error_message);
            return s;
        }

        op->type = CREATE_OP;
        return s;
    } else if (d->g == RELATIONAL_INSERT) {
        status s;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the parens
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);
        char* tbl_name = strtok(args, comma);

        // Find table and check if it's valid
        int tbl_idx = find_table(tbl_name);
        if (tbl_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find table\n";
            log_err(s.error_message);
            return s;
        }

        // Advance pointer to actual values
        char* vals = (char*)(args + strlen(tbl_name) + 1);
        
        s = relational_insert(tbl_idx, vals, op);

        // free(str_cpy);
        return s;
    } else if (d->g == SELECT_TYPE1_COLUMN) {
        status s;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);
        op->name1[strlen(var_name)] = '\0';

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);

        char vals[strlen(args)];
        strcpy(vals, args);

        char* arg = strtok(args, comma);
        char col_name[strlen(arg)];
        strcpy(col_name, arg);

        int tbl_idx = find_table_from_col_name(arg);
        if (tbl_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find table\n";
            log_err(s.error_message);
            return s;
        }
        table* table1 = global_db->tables[tbl_idx];

        int col_idx = find_column(table1, col_name);
        if (col_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find column\n";
            log_err(s.error_message);
            return s;
        }

        strtok(vals, comma);
        char* val1 = strtok(NULL, comma);
        int lower = create_lower_bound(val1);
        char* val2 = strtok(NULL, comma);
        int upper = create_upper_bound(val2);

        op->type = SELECT;
        op->columns = (column**)(table1->col + col_idx);
        op->lower = lower;
        op->upper = upper;

        s.code = OK;
        return s;
    } else if (d->g == SELECT_TYPE2_COLUMN) {
        status s;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);
        op->name1[strlen(var_name)] = '\0';

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);

        char* arg = strtok(args, comma);
        flush_queue(&queue, arg);
        op->result1 = find_result(arg);
        arg = strtok(NULL, comma);
        flush_queue(&queue, arg);
        op->result2 = find_result(arg);

        if (!op->result1 || !op->result2) {
            s.code = ERROR;
            s.error_message = "Cannot find previous result\n";
            return s;
        }

        char* val1 = strtok(NULL, comma);
        int lower = create_lower_bound(val1);
        char* val2 = strtok(NULL, comma);
        int upper = create_upper_bound(val2);

        op->type = SELECT;
        op->lower = lower;
        op->upper = upper;

        s.code = OK;
        return s;
    } else if (d->g == PROJECT_COLUMN) {
        status s;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);
        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);
        op->name1[strlen(var_name)] = '\0';

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);

        char* arg = strtok(args, comma);
        char col_name[strlen(arg)];
        strcpy(col_name, arg);
        char* name = strtok(NULL, comma);
        flush_queue(&queue, name);
        op->result1 = find_result(name);

        int tbl_idx = find_table_from_col_name(arg);
        if (tbl_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find table\n";
            log_err(s.error_message);
            return s;
        }
        table* table1 = global_db->tables[tbl_idx];

        int col_idx = find_column(table1, col_name);
        if (col_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find column\n";
            log_err(s.error_message);
            return s;
        }

        op->type = PROJECT;
        op->columns = (column**)(table1->col + col_idx);

        if (!op->result1) {
            s.code = ERROR;
            s.error_message = "Cannot find previous result\n";
            log_err(s.error_message);
            return s;
        }

        s.code = OK;
        return s;
    } else if (d->g == TUPLE_RESULT) {
        status s;

        tuples* tups = init_tuples();
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the parens
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);
        char* vec_name;
        while((vec_name = strtok(args, comma))) {
            char vec_name_copy[strlen(vec_name)];
            strcpy(vec_name_copy, vec_name);
            flush_queue(&queue, vec_name_copy);
            result* res = NULL;
            s = prepare_result(vec_name_copy, &res);
            if (s.code != OK) {
                log_err(s.error_message);
                return s;
            }
            tups->payloads[tups->num_cols] = res->payload;
            tups->num_cols++;
            tups->num_rows = res->num_tuples;
            tups->type = res->type;
            args = NULL;
        }
        
        op->type = TUPLE;
        op->tups = tups;

        s.code = OK;
        return s;
    } else if (d->g == AVG_RESULT) {
        status s;

        op->type = AGGREGATE;
        op->agg = AVG;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* vec_name = strtok(NULL, close_paren);
        flush_queue(&queue, vec_name);

        s = prepare_result(vec_name, &(op->result1));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        return s;
    } else if (d->g == MAX_RESULT) {
        status s;

        op->type = AGGREGATE;
        op->agg = MAX;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* vec_name = strtok(NULL, close_paren);
        flush_queue(&queue, vec_name);

        s = prepare_result(vec_name, &(op->result1));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        return s;
    } else if (d->g == MIN_RESULT) {
        status s;

        op->type = AGGREGATE;
        op->agg = MIN;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* vec_name = strtok(NULL, close_paren);
        flush_queue(&queue, vec_name);

        s = prepare_result(vec_name, &(op->result1));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        return s;
    } else if(d->g == CNT_RESULT) {
        status s;

        op->type = AGGREGATE;
        op->agg = CNT;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* vec_name = strtok(NULL, close_paren);
        flush_queue(&queue, vec_name);

        s = prepare_result(vec_name, &(op->result1));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        return s;
    } else if (d->g == SHUTDOWN_SERVER) {
        status s;

        op->type = SHUTDOWN;

        s.code = OK;
        return s;
    } else if (d->g == CREATE_BTREE) {
        status s;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str));
        strncpy(str_cpy, str, strlen(str));
        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);
        // This gives us everything inside the parens
        strtok(var_name, open_paren);
        char* args = strtok(NULL, close_paren);
        strtok(args, comma);
        char* arg = strtok(NULL, comma);

        char col_name[strlen(arg)];
        strcpy(col_name, arg);

        int tbl_idx = find_table_from_col_name(arg);
        if (tbl_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find table\n";
            log_err(s.error_message);
            return s;
        }
        table* table1 = global_db->tables[tbl_idx];

        int col_idx = find_column(table1, col_name);
        if (col_idx == -1) {
            s.code = ERROR;
            s.error_message = "Cannot find column\n";
            log_err(s.error_message);
            return s;
        }
        column* col1 = table1->col[col_idx];
        // Check if index exists for this column
        if (col1->index) {
            s.code = ERROR;
            s.error_message = "Index already exists for this column\n";
            log_err(s.error_message);
            return s;
        }

        s = create_index(col1, B_PLUS_TREE);
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        op->type = CREATE_OP;
        s.code = OK;
        return s;

    } else if (d->g == ADD_RESULT) {
        status s;

        op->type = ADD;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);
        char* vec_name = strtok(args, comma);
        char var_name1[strlen(vec_name)];
        strcpy(var_name1, vec_name);
        flush_queue(&queue, var_name1);

        vec_name = strtok(NULL, comma);
        char var_name2[strlen(vec_name)];
        strcpy(var_name2, vec_name);
        flush_queue(&queue, var_name2);

        s = prepare_result(var_name1, &(op->result1));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        s = prepare_result(var_name2, &(op->result2));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        if (op->result1->num_tuples != op->result2->num_tuples) {
            s.code = ERROR;
            s.error_message = "Vectors must have same length\n";
            log_err(s.error_message);
            return s;
        }

        s.code = OK;
        return s;
    } else if (d->g == SUB_RESULT) {
        status s;

        op->type = SUB;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);
        char* var_name = strtok(str_cpy, "=");
        op->name1 = malloc(strlen(var_name)+1);
        strcpy(op->name1, var_name);

        // This gives us everything inside the parens
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);
        char* vec_name = strtok(args, comma);
        char var_name1[strlen(vec_name)];
        strcpy(var_name1, vec_name);
        flush_queue(&queue, var_name1);

        vec_name = strtok(NULL, comma);
        char var_name2[strlen(vec_name)];
        strcpy(var_name2, vec_name);
        flush_queue(&queue, var_name2);

        s = prepare_result(var_name1, &(op->result1));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        s = prepare_result(var_name2, &(op->result2));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        if (op->result1->num_tuples != op->result2->num_tuples) {
            s.code = ERROR;
            s.error_message = "Vectors must have same length\n";
            log_err(s.error_message);
            return s;
        }

        s.code = OK;
        return s;
    } else if (d->g == HASHJOIN) {
        status s;

        op->type = JOIN;
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);
        char* vec_name1 = strtok(str_cpy, comma);
        op->name1 = malloc(strlen(vec_name1)+1);
        strcpy(op->name1, vec_name1);
        char* vec_name2 = strtok(NULL, "=");
        op->name2 = malloc(strlen(vec_name2)+1);
        strcpy(op->name2, vec_name2);
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);

        char* vec_name3 = strtok(args, comma);
        char vec_name_copy3[strlen(vec_name3)];
        strcpy(vec_name_copy3, vec_name3);
        flush_queue(&queue, vec_name_copy3);
        s = prepare_result(vec_name_copy3, &(op->result1));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }
        char* vec_name4 = strtok(NULL, comma);
        char vec_name_copy4[strlen(vec_name4)];
        strcpy(vec_name_copy4, vec_name4);
        flush_queue(&queue, vec_name_copy4);
        s = prepare_result(vec_name_copy4, &(op->result2));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        char* vec_name5 = strtok(NULL, comma);
        char vec_name_copy5[strlen(vec_name5)];
        strcpy(vec_name_copy5, vec_name5);
        flush_queue(&queue, vec_name_copy5);
        s = prepare_result(vec_name_copy5, &(op->result3));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        char* vec_name6 = strtok(NULL, comma);
        char vec_name_copy6[strlen(vec_name6)];
        strcpy(vec_name_copy6, vec_name6);
        flush_queue(&queue, vec_name_copy6);
        s = prepare_result(vec_name_copy6, &(op->result4));
        if (s.code != OK) {
            log_err(s.error_message);
            return s;
        }

        s.code = OK;
        return s;
    } else if (d->g == SHARED) {
        status s;

        op->type = SHARED_SCAN;

        s.code = OK;
        return s;
    }

    // Should have been caught earlier...
    status fail;
    fail.code = ERROR;
    return fail;
}
