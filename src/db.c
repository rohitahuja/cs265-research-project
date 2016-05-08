#include <stdio.h>
#include <string.h>
#include <limits.h>
#include "db.h"
#include "bpt.h"

// TODO(USER): Here we provide an incomplete implementation of the create_db.
// There will be changes that you will need to include here.
status create_db(const char* db_name, db** db) {
	status s;

	*db = malloc(sizeof(struct db));

    if (!*db) {
    	s.code = ERROR;
		s.error_message = "DB allocation failed\n";
		return s;
    }

    (*db)->name = malloc(strlen(db_name)+1);
    strcpy((char *)(*db)->name, db_name);
    (*db)->table_count = 0;
    (*db)->tables = (table**) calloc(DEFAULT_NUM_TABLES, sizeof(struct table*));

    if (!(*db)->tables) {
    	s.code = ERROR;
    	s.error_message = "Table pointer allocation failed\n";
    	free(*db);
    	return s;
    }

    s.code = OK;
    return s;
}

status create_table(db* db, const char* name, size_t num_columns, table** table) {
	status s;

    if (!db) {
        s.code = ERROR;
        s.error_message = "DB don't exist\n";
        return s;
    }

	*table = malloc(sizeof(struct table));

	if (!(*table)) {
    	s.code = ERROR;
		s.error_message = "Table allocation failed\n";
		return s;
    }

	(*table)->name = malloc(strlen(name)+1);
	strcpy((char *)(*table)->name, name);

	(*table)->col_count = 0;
    (*table)->leading_idx = -1;
	(*table)->col = (column**) calloc(num_columns, sizeof(struct column*));

	if (!(*table)->col) {
    	s.code = ERROR;
    	s.error_message = "Column pointer allocation failed\n";
    	free(*table);
    	return s;
    }

    db->tables[db->table_count] = (*table);
    db->table_count++;

	s.code = OK;
	return s;

}

status create_column(table *table, const char* name, column** col, bool sorted) {
	status s;

    if (!table) {
        s.code = ERROR;
        s.error_message = "Table does not exist\n";
        return s;
    }

	*col = malloc(sizeof(struct column));

	if (!*col) {
    	s.code = ERROR;
		s.error_message = "Column allocation failed\n";
		return s;
    }

	(*col)->name = malloc(strlen(name)+1);
	strcpy((char *)(*col)->name, name);

	(*col)->data = (int*) calloc(DEFAULT_NUM_VALS, sizeof(int));
	(*col)->index = NULL;
    (*col)->data_count = 0;
    (*col)->leading = sorted;

    if (sorted) {
        table->leading_idx = table->col_count;
    }

    table->col[table->col_count] = *col;
    table->col_count++;

	s.code = OK;
	return s;
}

status create_index(column* col, IndexType type) {
    status s;
    (void)type;// TODO ENSURE THIS IS BPT
    col->index = malloc(sizeof(struct column_index));
    col->index->type = B_PLUS_TREE;
    col->index->index = NULL;
    s = build_secondary_bpt_index(col);
    return s;
}

status fetch(column *col, int* indices, size_t val_count, result **r) {
    status s;
    
    (*r)->payload = calloc(val_count, sizeof(int));
    int* payload = (int*)(*r)->payload;
    (*r)->num_tuples = val_count;
    (*r)->type = INT;

    for(size_t i = 0; i < val_count; i++) {
        payload[i] = col->data[indices[i]];
    }
    
    s.code = OK;
    return s;
}

status col_insert(column *col, int data) {
    status s;

    size_t i = col->data_count;
    col->data[i] = data;
    col->data_count++;

    s.code = OK;
    return s;
}

status process_indexes(table* tbl) {
    status s;

    for(size_t i = 0; i < tbl->col_count; i++) {
        column* col = tbl->col[i];
        if (col->index) {
            //TODO asssert that index is bpt
            s = build_secondary_bpt_index(col);
            if (s.code != OK) {
                return s;
            }
        }
    }

    s.code = OK;
    return s;
}

status select_data(db_operator* query, result **r) {
    int lower = query->lower;
    int upper = query->upper;
    column* col = *(query->columns);
    if (col->leading || col->index) {
        return index_scan(lower, upper, col, r);
    } else {
        return col_scan(lower, upper, col, r);
    }
}

status index_scan(int lower, int upper, column *col, result **r) {
    status s;

    (*r)->payload = calloc(col->data_count, sizeof(int));
    (*r)->type = INT;
    (*r)->num_tuples = 0;

    if (col->index) {
        //TODO assert type is bpt col->index->type
        return find_range_bpt(col, *r, lower, upper);
    }

    s.code = ERROR;
    s.error_message = "No index specified\n";
    return s;
}

status col_scan(int lower, int upper, column *col, result **r) {
    status s;

    (*r)->payload = calloc(col->data_count, sizeof(int));
    int* payload = (int*)(*r)->payload;
    (*r)->type = INT;
    size_t j = 0;
    for(size_t i = 0; i < col->data_count; i++) {
        int data = col->data[i];
        int qualifies = check_data(data, lower, upper);
        payload[j] = i*qualifies;
        j += qualifies;
    }
    (*r)->num_tuples = j;
    
    s.code = OK;
    return s;
}

status vec_scan(db_operator* query, result** r) {
    status s;

    int lower = query->lower;
    int upper = query->upper;
    int* pos1 = (int*)query->result1->payload;
    int* val1 = (int*)query->result2->payload;
    size_t num_vals = query->result1->num_tuples;

    (*r)->payload = calloc(num_vals, sizeof(int));
    int* payload = (int*)(*r)->payload;
    (*r)->type = INT;
    size_t j = 0;
    for(size_t i = 0; i < num_vals; i++) {
        int data = val1[i];
        int qualifies = check_data(data, lower, upper);
        payload[j] = pos1[i]*qualifies;
        j += qualifies;
    }
    (*r)->num_tuples = j;

    s.code = OK;
    return s;
}

status add_col(int* vals1, int* vals2, size_t num_vals, result** r) {
    status s;

    (*r)->payload = calloc(num_vals, sizeof(long));
    long* payload = (long*)(*r)->payload;
    (*r)->num_tuples = num_vals;
    (*r)->type = LONG;
    for(size_t i = 0; i < num_vals; i++) {
        payload[i] = (long)vals1[i] + (long)vals2[i];
    }
    s.code = OK;
    return s;
}

status sub_col(int* vals1, int* vals2, size_t num_vals, result** r) {
    status s;

    (*r)->payload = calloc(num_vals, sizeof(long));
    long* payload = (long*)(*r)->payload;
    (*r)->num_tuples = num_vals;
    (*r)->type = LONG;
    for(size_t i = 0; i < num_vals; i++) {
        payload[i] = (long)vals1[i] - (long)vals2[i];
    }

    s.code = OK;
    return s;
}

status max_col(result* inter, result** r) {
    status s;

    (*r)->type = inter->type;
    (*r)->num_tuples = 1;

    if (inter->type == INT) {
        (*r)->payload = malloc(sizeof(int));
        int* vals = (int*)inter->payload;
        size_t num_vals = inter->num_tuples;
        int max = INT_MIN;
        for(size_t i = 0; i < num_vals; i++) {
            if (vals[i] > max) {
                max = vals[i];
            }
        }
        *((int*)(*r)->payload) = max;
    } else if (inter->type == LONG) {
        (*r)->payload = malloc(sizeof(long));
        long* vals = (long*)inter->payload;
        size_t num_vals = inter->num_tuples;
        long max = LONG_MIN;
        for(size_t i = 0; i < num_vals; i++) {
            if (vals[i] > max) {
                max = vals[i];
            }
        }
        *((long*)(*r)->payload) = max;
    } else {
        s.code = ERROR;
        s.error_message = "Intermediate result has no type\n";
        return s;
    }
    
    s.code = OK;
    return s;
}

status min_col(result* inter, result** r) {
    status s;

    (*r)->type = inter->type;
    (*r)->num_tuples = 1;

    if (inter->type == INT) {
        (*r)->payload = malloc(sizeof(int));
        int* vals = (int*)inter->payload;
        size_t num_vals = inter->num_tuples;
        int min = INT_MAX;
        for(size_t i = 0; i < num_vals; i++) {
            if (vals[i] < min) {
                min = vals[i];
            }
        }
        *((int*)(*r)->payload) = min;
    } else if (inter->type == LONG) {
        (*r)->payload = malloc(sizeof(long));
        long* vals = (long*)inter->payload;
        size_t num_vals = inter->num_tuples;
        long min = LONG_MAX;
        for(size_t i = 0; i < num_vals; i++) {
            if (vals[i] < min) {
                min = vals[i];
            }
        }
        *((long*)(*r)->payload) = min;
    } else {
        s.code = ERROR;
        s.error_message = "Intermediate result has no type\n";
        return s;
    }
    
    s.code = OK;
    return s;
}

status avg_col(result* inter, result** r) {
    status s;

    (*r)->payload = malloc(sizeof(long double));
    long double* payload = (long double*)(*r)->payload;
    int* vals = inter->payload;
    (*r)->type = LONG_DOUBLE;

    long double avg = 0.0;
    long double num_vals = (long double)inter->num_tuples;
    for(size_t i = 0; i < num_vals; i++) {
        avg += ((long double)vals[i])/num_vals;
    }

    (*r)->num_tuples = 1;
    *payload = avg;

    s.code = OK;
    return s;
}

status count_col(size_t num_vals, result** r) {
    status s;

    (*r)->payload = malloc(sizeof(int));
    *((int*)(*r)->payload) = num_vals;
    (*r)->type = INT;
    (*r)->num_tuples = 1;

    s.code = OK;
    return s;
}
