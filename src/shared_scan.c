#include  "shared_scan.h"

select_queue* init_select_queue() {
    select_queue* queue = malloc(sizeof(struct select_queue));
    queue->buffer = calloc(DEFAULT_SHARED_SCAN_BUFFER_SIZE, sizeof(struct thread_args*));
    queue->col = NULL;
    queue->buffer_count = 0;
    for(int i = 0; i < DEFAULT_SHARED_SCAN_BUFFER_SIZE; i++) {
        queue->buffer[i] = malloc(sizeof(struct thread_args));
    }
    return queue;
}

status subselect_data(db_operator* query, result* res, size_t start, size_t end) {
	status s;

	int* payload = (int*)res->payload;
	size_t j = res->num_tuples;
	column* col = *(query->columns);
	for (size_t i = start; i < end; i++) {
		int data = col->data[i];
		int qualifies = check_data(data, query->lower, query->upper);
		payload[j] = i*qualifies;
		j += qualifies;
	}
	res->num_tuples = j;

	s.code = OK;
	return s;
}

void* run_query(void* a) {
	log_info("Thread #: %lu\n", (unsigned long) pthread_self());
	thread_args* args = (thread_args*) a;
	(void) subselect_data(args->query, args->res, args->start, args->end);
	return 0;
}

void init_results(column* col, select_queue* queue) {
	for (size_t i = 0; i < queue->buffer_count; i++) {
		queue->buffer[i]->res = malloc(sizeof(struct result));
		queue->buffer[i]->res->payload = calloc(col->data_count, sizeof(int));
		queue->buffer[i]->res->type = INT;
		queue->buffer[i]->res->num_tuples = 0;
	}
}

status flush_queue(select_queue** queue, char* name) {
	status s;

	if (!sscan) {
		s.code = OK;
		return s;
	}

    for(size_t i = 0; i < (*queue)->buffer_count; i++) {
        char* vec_name = (*queue)->buffer[i]->query->name1;
        if ((strlen(vec_name) == strlen(name)) && strcmp(vec_name, name) == 0) {
            return shared_scan_select((*queue)->col, queue);
        }
    }

    s.code = OK;
    return s;
}

status shared_scan_select(column* col, select_queue** queue) {
	status s;

	pthread_t threads[(*queue)->buffer_count];
	init_results(col, *queue);
	for (size_t i = 0; i < col->data_count; i += PAGESIZE) {
		for (size_t j = 0; j < (*queue)->buffer_count; j++) {
			(*queue)->buffer[j]->start = i;
			(*queue)->buffer[j]->end = i + PAGESIZE < col->data_count ? i + PAGESIZE : col->data_count;
			int rc = pthread_create(&threads[j], NULL, run_query, (void*) (*queue)->buffer[j]);

			if (rc) {
				s.code = ERROR;
				s.error_message = "Error creating pthread\n";
				return s;
			}
		}
		for (size_t j = 0; j < (*queue)->buffer_count; j++) {
			(void) pthread_join(threads[j], NULL);
		}
	}

	for(size_t i = 0; i < (*queue)->buffer_count; i++) {
        int idx = catalogs[0]->var_count;
        catalogs[0]->names[idx] = (*queue)->buffer[i]->query->name1;
        catalogs[0]->results[idx] = (*queue)->buffer[i]->res;
        catalogs[0]->var_count++;
    }

	*queue = init_select_queue();

	s.code = OK;
	return s;
}