#include "helpers.h"
#include "utils.h"
#include <ctype.h>

// This tells the linker that there exists a global_db and catalog external
// from this file.
extern db* global_db;
extern catalog** catalogs;

// PARSER FUNCTIONS

int find_table(char* tbl_name) {
    for(size_t i = 0; i < global_db->table_count; i++) {
        if (strcmp(global_db->tables[i]->name, tbl_name) == 0) {
            return i;
        }
    }
    return -1;
}

int find_column(table* tbl, char* col_name) {
    for (int i = 0; i < (int)tbl->col_count; i++) {
        if (strcmp(tbl->col[i]->name, col_name) == 0) {
            return i;
        }
    }
    return -1;
}

int find_table_from_col_name(char* col_name) {
    char tbl_name[strlen(col_name)];
    memset(tbl_name, '\0', strlen(col_name)); 
    strcat(tbl_name, strtok(col_name, "."));
    strcat(tbl_name, ".");
    strcat(tbl_name, strtok(NULL, "."));

    return find_table(tbl_name);
}

int create_lower_bound(char* val) {
    if (strcmp("null", val) == 0) {
        return INT_MIN;
    }
    return atoi(val);
}

int create_upper_bound(char* val) {
    if (strcmp("null", val) == 0) {
        return INT_MAX;
    }
    return atoi(val);
}

result* find_result(char* name) {
    for(size_t i = 0; i < catalogs[0]->var_count; i++) {
        if ((strlen(catalogs[0]->names[i]) == strlen(name)) && strncmp(catalogs[0]->names[i], name, strlen(catalogs[0]->names[i])) == 0) {
            return catalogs[0]->results[i];
        }
    }
    return NULL;
}

status prepare_result(char* var_name, result** r) {
    status s;

    *r = find_result(var_name);
    if (*r) {
        s.code = OK;
        return s;
    }

    char col_name[strlen(var_name)];
    strcpy(col_name, var_name);

    int tbl_idx = find_table_from_col_name(var_name);
    if (tbl_idx == -1) {
        s.code = ERROR;
        s.error_message = "Cannot find var\n";
        return s;
    }
    table* table1 = global_db->tables[tbl_idx];

    int col_idx = find_column(table1, col_name);
    if (col_idx == -1) {
        s.code = ERROR;
        s.error_message = "Cannot find var\n";
        return s;
    }
    column* col1 = table1->col[col_idx];

    *r = malloc(sizeof(struct result));
    (*r)->payload = col1->data;
    (*r)->num_tuples = col1->data_count;
    (*r)->type = INT;

    s.code = OK;
    return s;
}

status relational_insert(int tbl_idx, char* vals, db_operator* op) {
    table *table1 = global_db->tables[tbl_idx];

    char comma[2] = ",";
    status s;
    char* val = strtok(vals, comma);
    int i = 0;

    op->value1 = calloc(table1->col_count, sizeof(int));
    op->type = INSERT;
    op->tables = (table**)(global_db->tables + tbl_idx);
    op->columns = table1->col;
    
    while(val && i < (int)table1->col_count) {
        op->value1[i] = atoi(val);
        val = strtok(NULL, comma);
        i++;
    }

    if (val) {
        free(op->value1);

        s.code = ERROR;
        s.error_message = "Values exceed number of columns\n";
        return s;
    }

    if (i != (int)table1->col_count) {
        free(op->value1);

        s.code = ERROR;
        s.error_message = "Not enough values to insert\n";
        return s;
    }

    s.code = OK;
    return s;
}

// SERVER FUNCTIONS

catalog** init_catalogs() {
    catalog** new_catalogs = calloc(DEFAULT_NUM_CLIENTS_ALLOWED, sizeof(struct catalog*));
    for(int i=0; i < DEFAULT_NUM_CLIENTS_ALLOWED; i++) {
        new_catalogs[i] = malloc(sizeof(struct catalog));
        new_catalogs[i]->var_count = 0;
        for(int j=0; j < DEFAULT_CATALOG_RESULTS; j++) {
            new_catalogs[i]->names[j] = NULL;
            new_catalogs[i]->results[j] = NULL;    
        } 
    }
    return new_catalogs;
}

status write_column(FILE* f, column* col1) {
    status s;

    fprintf(f, "%s\n", col1->name);
    if (col1->leading) {
        fprintf(f, "leading\n");
    } else {
        fprintf(f, "none\n");
    }

    if (col1->index && col1->index->type == SORTED) {
        fprintf(f, "sorted\n");
    } else if (col1->index && col1->index->type == B_PLUS_TREE) {
        fprintf(f, "b_plus_tree\n");
    } else{
        fprintf(f, "none\n");
    }
    fprintf(f, "%zu\n", col1->data_count);
    for(int i = 0; i < (int)col1->data_count; i++) {
        fprintf(f, "%d\n", col1->data[i]);
    }

    // Free memory
    free((char*)(col1->name));
    free(col1->data);
    free(col1);

    s.code = OK;
    return s;
}

status write_table(FILE* f, table* table1) {
    status s;
    
    fprintf(f, "%s\n", table1->name);
    fprintf(f, "%zu\n", table1->col_count);
    for(int i = 0; i < (int)table1->col_count; i++) {
        s = write_column(f, table1->col[i]);
        if (s.code != OK) {
            return s;
        }
    }

    free((char*)(table1->name));
    free(table1->col);
    free(table1);

    s.code = OK;
    return s;
}

status write_db(FILE* f) {
    status s;

    fprintf(f, "%s\n", global_db->name);
    fprintf(f, "%zu\n", global_db->table_count);
    for(int i = 0; i < (int)global_db->table_count; i++) {
        s = write_table(f, global_db->tables[i]);
        if (s.code != OK) {
            return s;
        }
    }

    free((char*)(global_db->name));
    free(global_db->tables);
    free(global_db);

    s.code = OK;
    return s;
}

status persist_data() {
    status s;

    if (!global_db) {
        s.code = OK;
        return s;
    }

    FILE* f = fopen("db.txt", "w");
    if (f == NULL) {
        f = fopen("db.txt", "aw");
    }
    s = write_db(f);
    fclose(f);
    if (s.code != OK) {
        return s;
    }

    return s;
}

status read_column(FILE* f, table** tbl1, column** col1) {
    status s;

    char * line = NULL;
    size_t len = 0;
    ssize_t read = getline(&line, &len, f);
    if (read == -1) {
        s.code = ERROR;
        s.error_message = "Error reading data on disk\n";
        return s;
    }
    s = create_column(*tbl1, strtok(line, "\n"), col1, false);
    if (s.code != OK) {
        return s;
    }

    read = getline(&line, &len, f);
    if (read == -1) {
        s.code = ERROR;
        s.error_message = "Error reading data on disk\n";
        return s;
    }
    if (strncmp(line, "leading", 7) == 0) {
        (*col1)->leading = true;
        (*tbl1)->leading_idx = (*tbl1)->col_count-1;
    } 

    read = getline(&line, &len, f);
    if (read == -1) {
        s.code = ERROR;
        s.error_message = "Error reading data on disk\n";
        return s;
    }

    IndexType type = NONE;
    if (strncmp(line, "sorted", 6) == 0) {
        type = SORTED;
    } else if (strncmp(line, "b_plus_tree", 11) == 0) {
        type = B_PLUS_TREE;
    }

    read = getline(&line, &len, f);
    if (read == -1) {
        s.code = ERROR;
        s.error_message = "Error reading data on disk\n";
        return s;
    }
    size_t num_data = (size_t) atoi(line);

    for (int i = 0; i < (int)num_data; i++) {
        read = getline(&line, &len, f);
        if (read == -1) {
            s.code = ERROR;
            s.error_message = "Error reading data on disk\n";
            return s;
        }
        s = col_insert(*col1, atoi(line));
        if (s.code != OK) {
            return s;
        }
    }

    if (type != NONE) {
        s = create_index(*col1, type);
        if (s.code != OK) {
            return s;
        }
    }

    s.code = OK;
    return s;
}

status read_table(FILE* f, table** tbl1) {
    status s;

    char * line = NULL;
    size_t len = 0;
    ssize_t read = getline(&line, &len, f);
    if (read == -1) {
        s.code = ERROR;
        s.error_message = "Error reading data on disk\n";
        return s;
    }
    char* temp = strtok(line, "\n");
    char tbl_name[strlen(temp)];
    strcpy(tbl_name, temp);
    read = getline(&line, &len, f);
    if (read == -1) {
        s.code = ERROR;
        s.error_message = "Error reading data on disk\n";
        return s;
    }
    size_t num_columns = (size_t) atoi(line);
    s = create_table(global_db, tbl_name, num_columns, tbl1);
    if (s.code != OK) {
        return s;
    }

    for (int i = 0; i < (int) num_columns; i++) {
        column* col1 = NULL;
        s = read_column(f, tbl1, &col1);
        if (s.code != OK) {
            return s;
        }
        (*tbl1)->col[i] = col1;
    }

    s.code = OK;
    return s;
}

status read_db(FILE* f) {
    status s;

    char * line = NULL;
    size_t len = 0;
    ssize_t read = getline(&line, &len, f);

    if (read == -1) {
        s.code = OK;
        return s;
    }
    s = create_db(strtok(line, "\n"), &global_db);
    if (s.code != OK) {
        return s;
    }

    read = getline(&line, &len, f);
    if (read == -1) {
        s.code = ERROR;
        s.error_message = "Error reading data on disk\n";
        return s;
    }
    size_t table_count = (size_t) atoi(line);
    for (int i = 0; i < (int)table_count; i++) {
        table* tbl1 = NULL;
        s = read_table(f, &tbl1);
        if (s.code != OK) {
            return s;
        }
        global_db->tables[i] = tbl1;
    }

    s.code = OK;
    return s;
}

status grab_persisted_data() {
    status s;

    FILE* f = fopen("db.txt", "r");
    
    // File doesn't exist
    if (f == NULL) {
        log_info("db.txt doesn't exist\n");
        s.code = OK;
        return s;
    }

    s = read_db(f);
    fclose(f);
    if (s.code != OK) {
        return s;
    }

    s.code = OK;
    return s;

}

void free_result(result* res) {
    free(res->payload);
    free(res);
}

status free_catalogs() {
    status s;
    for(int i=0; i < DEFAULT_NUM_CLIENTS_ALLOWED; i++) {
        for(int j=0; j < DEFAULT_CATALOG_RESULTS; j++) {
            if (!(catalogs[i]->names[j])) {
                break;
            }
            free(catalogs[i]->names[j]);
            free_result(catalogs[i]->results[j]);
        }
        free(catalogs[i]);
    }
    s.code = OK;
    return s;
}

db_operator* init_dbo() {
    db_operator *dbo = malloc(sizeof(struct db_operator));
    dbo->columns = NULL;
    dbo->tables = NULL;
    dbo->value1 = NULL;
    dbo->value2 = NULL;
    dbo->result1 = NULL;
    dbo->result2 = NULL;
    return dbo;
}

int binary_search(int* data, int target, int start, int end) {
    int mid;
    while(start != end) {
        mid = (start+end) >> 1;
        if (data[mid] <= target) {
            for(; mid < end-1 && data[mid] == data[mid+1]; mid++);
            start = mid + 1;
        } else {
            for(; mid > start && data[mid-1] == data[mid]; mid--);
            end = mid;
        }
    }
    return start;
}

int check_data(int data, int lower, int upper) {
    return (int)(lower <= data && data < upper);
}

tuples* init_tuples() {
    tuples* tups = malloc(sizeof(struct tuples));
    tups->payloads = calloc(DEFAULT_NUM_COLS, sizeof(int*));
    tups->num_cols = 0;
    tups->num_rows = 0;
    return tups;
}
