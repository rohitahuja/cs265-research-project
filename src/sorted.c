#include "sorted.h"


sorted_index* init_sorted_index() {
	sorted_index* sidx = malloc(sizeof(struct sorted_index));
	sidx->data = calloc(DEFAULT_NUM_VALS, sizeof(int));
	sidx->positions = calloc(DEFAULT_NUM_VALS, sizeof(int));
	sidx->data_count = 0;
	return sidx;
}

status find_range_leading_sorted(column* col, result *r, int lower, int upper) {
	log_info("Searching in leading column\n");
	int start = binary_search(col->data, lower, 0, col->data_count);
	int end = binary_search(col->data, upper, start, col->data_count);

	for(; end < (int)col->data_count && col->data[end] == col->data[end+1]; end++);

	int* payload = (int*)r->payload;
	size_t i;
	for(i = 0; start <= end; i++, start++) {
		payload[i] = start;
	}

	r->num_tuples = i;
	r->type = INT;

	status s;
	s.code = OK;
	return s;
}

status find_range_secondary_sorted(sorted_index* sidx, result* r, int lower, int upper) {
	log_info("Searching in secondary sorted column\n");
	status s;
	int start = 0;
	int end = sidx->data_count;
	size_t i, j;

	j = binary_search(sidx->data, lower, start, end);

	int* payload = (int*)r->payload;
	for(i = 0; j < sidx->data_count && sidx->data[j] < upper; j++, i++) {
		payload[i] = sidx->positions[j];
	}
	r->num_tuples = i;

	s.code = OK;
	return s;
}

status insert_secondary_sorted(sorted_index** sidx, int data, int position) {
	log_info("Inserting in secondary sorted column\n");
	if (!(*sidx)) {
		*sidx = init_sorted_index();
	}

	status s;
	size_t i;
	for (i = 0; i < (*sidx)->data_count && (*sidx)->data[i] < data; i++);

	int pre_data = data;
	int pre_pos = position;

	for (; i < (*sidx)->data_count; i++) {
		int temp1 = (*sidx)->data[i];
		int temp2 = (*sidx)->positions[i];

		(*sidx)->data[i] = pre_data;
		(*sidx)->positions[i] = pre_pos;

		pre_data = temp1;
		pre_pos = temp2;
	}
	(*sidx)->data[i] = pre_data;
	(*sidx)->positions[i] = pre_pos;
	(*sidx)->data_count++;

	s.code = OK;
	return s;
}

int secondary_partition(int* data, int* pos, int l, int r) {
	int pivot, i, j, t;
	pivot = data[l];
	i = l; j = r+1;
	
	while(true) {
		do ++i; while(data[i] <= pivot && i <= r);
		do --j; while(data[j] > pivot);
		if( i >= j ) break;
		t = data[i]; data[i] = data[j]; data[j] = t;
		t = pos[i]; pos[i] = pos[j]; pos[j] = t;
	}
	t = data[l]; data[l] = data[j]; data[j] = t;
	t = pos[l]; pos[l] = pos[j]; pos[j] = t;

	return j;
}

void secondary_quicksort(int* data, int* pos, int l, int r) {
	int j;
   	if(l < r) {
		j = secondary_partition(data, pos, l, r);

		secondary_quicksort(data, pos, l, j-1);
		secondary_quicksort(data, pos, j+1, r);
	}
}

status secondary_sort(column* col, int* data, int* pos) {
	status s;

	secondary_quicksort(data, pos, 0, col->data_count-1);

	s.code = OK;
	return s;
}

status build_secondary_sorted_index(column *col) {
    status s;

    sorted_index** sidx = (sorted_index**) (&(col->index->index));
    int* data = calloc(col->data_count, sizeof(int));
    int* pos = calloc(col->data_count, sizeof(int));

    for (size_t i = 0; i < col->data_count; i++) {
        data[i] = col->data[i];
        pos[i] = i;
    }

    s = secondary_sort(col, data, pos);
    if (s.code != OK) {
        return s;
    }

    (*sidx)->data = data;
    (*sidx)->positions = pos;

    s.code = OK;
    return s;
}

int leading_partition(table* tbl, int* data, int l, int r) {
	int pivot, i, j, t;
	pivot = data[l];
	i = l; j = r+1;
	
	while(true) {
		do ++i; while(data[i] <= pivot && i <= r);
		do --j; while(data[j] > pivot);
		if( i >= j ) break;

		for (size_t k = 0; k < tbl->col_count; k++) {
			column* col = tbl->col[k];
			t = col->data[i]; col->data[i] = col->data[j]; col->data[j] = t;
		}
	}

	for (size_t k = 0; k < tbl->col_count; k++) {
		column* col = tbl->col[k];
		t = col->data[l]; col->data[l] = col->data[j]; col->data[j] = t;
	}

	return j;
}

void leading_quicksort(table* tbl, int* data, int l, int r) {
	int j;
   	if(l < r) {
		j = leading_partition(tbl, data, l, r);

		leading_quicksort(tbl, data, l, j-1);
		leading_quicksort(tbl, data, j+1, r);
	}
}

status leading_sort(table* tbl, column* leading_col) {
	status s;

	leading_quicksort(tbl, leading_col->data, 0, leading_col->data_count-1);

	s.code = OK;
	return s;
}

status leading_column_sort(table* tbl) {
    status s;

    column* leading_col = tbl->col[tbl->leading_idx];
    s = leading_sort(tbl, leading_col);
    if (s.code != OK) {
        return s;
    }

    s.code = OK;
    return s;
}

