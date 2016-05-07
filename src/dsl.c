#include "dsl.h"

// Create Commands
// Matches: create(db, <db_name>);
const char* create_db_command = "^create\\(db\\,\\\"[a-zA-Z0-9_]+\\\"\\)";

// Matches: create(tbl, <table_name>, <db_name>, <col_count>);
const char* create_table_command = "^create\\(tbl\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,[0-9]+\\)";

// Matches: create(col, <col_name>, <tbl_var>, sorted);
const char* create_col_command_sorted = "^create\\(col\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,sorted)";

// Matches: create(col, <col_name>, <tbl_var>, unsorted);
const char* create_col_command_unsorted = "^create\\(col\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,unsorted)";

// Matches: relational_insert(<db_name>.<tbl_name>, <col1_val>, <col2_val>, ...)
const char* relational_insert_command = "^relational_insert\\([a-zA-Z0-9_\\.]+\\,[-0-9\\,]*\\)";

// Matches: load("<file_path>")
const char* bulk_load_command = "^load\\(\\\"[^[:space:]]+\\\"\\)"; 

// Matches: <var_name>=select(<col_name>, <lower_bound>, <upper_bound>)
const char* select_type1_column_command = "^[a-zA-Z0-9_]+=select\\([a-zA-Z0-9_\\.]+\\,[-0-9\\nul]+\\,[-0-9\\nul]+\\)";

// Matches: <var_name>=select(<pos_var>, <col_var>, <lower_bound>, <upper_bound>)
const char* select_type2_column_command = "^[a-zA-Z0-9_]+=select\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\,[-0-9\\nul]+\\,[-0-9\\nul]+\\)";

// Matches: <var_name>=fetch(<col_name>, <projection_var_name>)
const char* project_column_command = "^[a-zA-Z0-9_]+=fetch\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_]+\\)";

// Matches: tuple(<var_name>)
const char* tuple_result_command = "^tuple\\([a-zA-Z0-9_\\.\\,]+\\)";

// Matches: avg(<var_name>)
const char* avg_result_command = "^[a-zA-Z0-9_]+=avg\\([a-zA-Z0-9_\\.]+\\)";

// Matches: max(<var_name>)
const char* max_result_command = "^[a-zA-Z0-9_]+=max\\([a-zA-Z0-9_\\.]+\\)";

// Matches: min(<var_name>)
const char* min_result_command = "^[a-zA-Z0-9_]+=min\\([a-zA-Z0-9_\\.]+\\)";

// Matches: add(<vec_name1>,<vec_name2>)
const char* add_result_command = "^[a-zA-Z0-9_]+=add\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// Matches: sub(<vec_name1>,<vec_name2>)
const char* sub_result_command = "^[a-zA-Z0-9_]+=sub\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// Matches: add(<var_name>)
const char* cnt_result_command = "^[a-zA-Z0-9_]+=count\\([a-zA-Z0-9_\\.]+\\)";

// Matches: shutdown
const char* shutdown_server_command = "^shutdown";

// create(idx,awesomebase.grades.student_id,btree)
const char* create_btree_command = "^create\\(idx\\,[a-zA-Z0-9_\\.]+\\,btree\\)";

// <vec_pos1_out>,<vec_pos2_out>=hashjoin(<vec_val1>,<vec_pos1>,<vec_val2>,<vec_pos2>)
const char* hashjoin_command = "^[a-zA-Z0-9_]+,[a-zA-Z0-9_]+=hashjoin\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// shared
const char* shared_scan_command = "^shared";

// TODO(USER): You will need to update the commands here for every single command you add.
dsl** dsl_commands_init(void)
{
    dsl** commands = calloc(NUM_DSL_COMMANDS, sizeof(dsl*));

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        commands[i] = malloc(sizeof(dsl));
    }

    // Assign the create commands
    commands[0]->c = create_db_command;
    commands[0]->g = CREATE_DB;

    commands[1]->c = create_table_command;
    commands[1]->g = CREATE_TABLE;

    commands[2]->c = create_col_command_sorted;
    commands[2]->g = CREATE_COLUMN;

    commands[3]->c = create_col_command_unsorted;
    commands[3]->g = CREATE_COLUMN;

    commands[4]->c = relational_insert_command;
    commands[4]->g = RELATIONAL_INSERT;

    commands[5]->c = bulk_load_command;
    commands[5]->g = BULK_LOAD;

    commands[6]->c = select_type1_column_command;
    commands[6]->g = SELECT_TYPE1_COLUMN;

    commands[7]->c = project_column_command;
    commands[7]->g = PROJECT_COLUMN;

    commands[8]->c = tuple_result_command;
    commands[8]->g = TUPLE_RESULT;

    commands[9]->c = avg_result_command;
    commands[9]->g = AVG_RESULT;

    commands[10]->c = max_result_command;
    commands[10]->g = MAX_RESULT;

    commands[11]->c = min_result_command;
    commands[11]->g = MIN_RESULT;

    commands[12]->c = add_result_command;
    commands[12]->g = ADD_RESULT;

    commands[13]->c = cnt_result_command;
    commands[13]->g = CNT_RESULT;

    commands[14]->c = shutdown_server_command;
    commands[14]->g = SHUTDOWN_SERVER;

    commands[15]->c = create_btree_command;
    commands[15]->g = CREATE_BTREE;

    commands[16]->c = select_type2_column_command;
    commands[16]->g = SELECT_TYPE2_COLUMN;

    commands[17]->c = sub_result_command;
    commands[17]->g = SUB_RESULT;

    commands[18]->c = hashjoin_command;
    commands[18]->g = HASHJOIN;    

    commands[19]->c = shared_scan_command;
    commands[19]->g = SHARED;    

    return commands;
}
