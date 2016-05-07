#ifndef SORTED_H__
#define SORTED_H__

#include <stdio.h>
#include "cs165_api.h"
#include "helpers.h"
#include "utils.h"

typedef struct sorted_index {
	int* data;
	int* positions;
	size_t data_count;
} sorted_index;

int binary_search(int* data, int target, int start, int end);
status find_range_secondary_sorted(sorted_index* sidx, result* r, int lower, int upper);
status insert_secondary_sorted(sorted_index** sidx, int data, int position);
status find_range_leading_sorted(column* col, result *r, int lower, int upper);
status sort_column(column* col, int* data, int* pos);
status leading_column_sort(table* tbl);
status build_secondary_sorted_index(column *col);
sorted_index* init_sorted_index();

#endif // SORTED_H__