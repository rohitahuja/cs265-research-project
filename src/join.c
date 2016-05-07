#include "join.h"

size_t min(size_t a, size_t b) {
	return a < b ? a : b;
}

size_t max(size_t a, size_t b) {
	return a > b ? a : b;
}

int hash(int key, size_t length) {
	return abs(key % (int)length);
}

bool threshold(result* pos1, result* pos2) {
	size_t num_tuples = min(pos1->num_tuples, pos2->num_tuples);
	return num_tuples < HASH_THRESHOLD/sizeof(int);
}

status join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2) {
	status s;

	if (threshold(pos1, pos2)) {
		if (pos1->num_tuples > pos2->num_tuples) {
			s = nested_join(pos1, val1, pos2, val2, r1, r2);
		} else {
			s = nested_join(pos2, val2, pos1, val1, r2, r1);	
		}	
	} else {
		s = partition_hash_join(pos1, val1, pos2, val2, r1, r2);	
	}

	return s;
}

status nested_join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2) {
	int* val1_payload = (int*)val1->payload;
	int* pos1_payload = (int*)pos1->payload;
	int* val2_payload = (int*)val2->payload;
	int* pos2_payload = (int*)pos2->payload;
	int* r1_payload = (int*)(*r1)->payload;
	int* r2_payload = (int*)(*r2)->payload;

	for(size_t i = 0; i < pos1->num_tuples; i++) {
		for(size_t j = 0; j < pos2->num_tuples; j++) {
			if (val1_payload[i] == val2_payload[j]) {
				r1_payload[(*r1)->num_tuples++] = pos1_payload[i];
				r2_payload[(*r2)->num_tuples++] = pos2_payload[j];
			}
		}
	}
	
	status s;
	s.code = OK;
	return s;
}

status hash_join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2) {
	status s;

	size_t htable_size = max(pos1->num_tuples, pos2->num_tuples);
	hash_table* htable = hash_table_init(htable_size);

	s = insert_hash_table(htable, pos2, val2);
	s = probe(htable, pos1, val1, r1, r2);

	return s;
}

partition* init_partition(int level, size_t num_tuples1, size_t num_tuples2) {
	partition* node = malloc(sizeof(struct partition));
	node->level = level;
	node->val1 = calloc(num_tuples1, sizeof(int));
	node->pos1 = calloc(num_tuples1, sizeof(int));
	node->val2 = calloc(num_tuples2, sizeof(int));
	node->pos2 = calloc(num_tuples2, sizeof(int));
	node->num_tuples1 = 0;
	node->num_tuples2 = 0;
	node->c1 = NULL;
	node->c2 = NULL;
	return node;
}

partition* init_root(result* pos1, result* val1, result* pos2, result* val2) {
	partition* root = init_partition(0, pos1->num_tuples, pos2->num_tuples);
	root->num_tuples1 = pos1->num_tuples;
	root->num_tuples2 = pos2->num_tuples;

	int* val1_payload = (int*)val1->payload;
	int* pos1_payload = (int*)pos1->payload;
	int* val2_payload = (int*)val2->payload;
	int* pos2_payload = (int*)pos2->payload;

	for(size_t i = 0; i < pos1->num_tuples; i++) {
		root->val1[i] = val1_payload[i];
		root->pos1[i] = pos1_payload[i];
	}

	for(size_t i = 0; i < pos2->num_tuples; i++) {
		root->val2[i] = val2_payload[i];
		root->pos2[i] = pos2_payload[i];
	}

	return root;
}

status partition_node(partition* node) {
	status s;

	size_t min_tuples = min(node->num_tuples1, node->num_tuples2);
	if (min_tuples < CACHESIZE/sizeof(int)) {
		s.code = OK;
		return s;
	}

	partition* c1 = init_partition(node->level+1, node->num_tuples1, node->num_tuples2);
	partition* c2 = init_partition(node->level+1, node->num_tuples1, node->num_tuples2);

	int mask = 1 << node->level;
	for(size_t i = 0; i < node->num_tuples1; i++) {
		if (mask & node->val1[i]) {
			c2->val1[c2->num_tuples1] = node->val1[i];
			c2->pos1[c2->num_tuples1++] = node->pos1[i];
		} else {
			c1->val1[c1->num_tuples1] = node->val1[i];
			c1->pos1[c1->num_tuples1++] = node->pos1[i];
		}
	}

	for(size_t i = 0; i < node->num_tuples2; i++) {
		if (mask & node->val2[i]) {
			c2->val2[c2->num_tuples2] = node->val2[i];
			c2->pos2[c2->num_tuples2++] = node->pos2[i];
		} else {
			c1->val2[c1->num_tuples2] = node->val2[i];
			c1->pos2[c1->num_tuples2++] = node->pos2[i];
		}
	}

	node->c1 = c1;
	node->c2 = c2;

	free(node->val1);
	free(node->pos1);
	free(node->val2);
	free(node->pos2);

	(void) partition_node(node->c1);
	(void) partition_node(node->c2);

	s.code = OK;
	return s;
}

status join_partitions(partition* node, result** r1, result** r2) {
	status s;

	if (node->c1 && node->c2) {
		join_partitions(node->c1, r1, r2);
		join_partitions(node->c2, r1, r2);
		s.code = OK;
		return s;
	}

	result val1, pos1, val2, pos2;

	val1.payload = node->val1;
	val1.num_tuples = node->num_tuples1;
	val1.type = INT;
	pos1.payload = node->pos1;
	pos1.num_tuples = node->num_tuples1;
	pos1.type = INT;

	val2.payload = node->val2;
	val2.num_tuples = node->num_tuples2;
	val2.type = INT;
	pos2.payload = node->pos2;
	pos2.num_tuples = node->num_tuples2;
	pos2.type = INT;

	if (node->num_tuples1 > node->num_tuples2) {
		hash_join(&pos1, &val1, &pos2, &val2, r1, r2);	
	} else {
		hash_join(&pos2, &val2, &pos1, &val1, r2, r1);	
	}

	s.code = OK;
	return s;
}

status partition_hash_join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2) {
	status s;

	partition* root = init_root(pos1, val1, pos2, val2);

	(void) partition_node(root);

	s = join_partitions(root, r1, r2);

	return s;
}

hash_table* hash_table_init (size_t htable_size) {
	hash_table* htable = malloc(sizeof(struct hash_table));
	htable->table = calloc(htable_size, sizeof(struct hash_entry*));
	htable->current_size = 0;
	htable->max_size = htable_size;


	return htable;
}

hash_entry* hash_entry_init (int key, int value) {
	hash_entry* entry = malloc(sizeof(struct hash_entry));
	entry->key = key;
	entry->value = value;
	entry->next = NULL;

	return entry;
}

status insert_hash_table(hash_table* htable, result* pos, result* val) {
	status s;


	int* pos_payload = (int*)pos->payload;
	int* val_payload = (int*)val->payload;

	for (size_t i = 0; i < pos->num_tuples; i++) {		
		hash_entry* entry = hash_entry_init(val_payload[i], pos_payload[i]);

		int i = hash(entry->key, htable->max_size);

		s = entry_insert(&htable->table[i], entry);
		htable->current_size++;
	}

	s.code = OK;
	return s;
}

status entry_insert(hash_entry** head, hash_entry* entry) {
	status s;

	entry->next = *head;
	*head = entry;

	s.code = OK;
	return s;
}

status probe(hash_table* htable, result* pos, result* val, result** r1, result** r2) {
	status s;

	int* pos_payload = (int*)pos->payload;
	int* val_payload = (int*)val->payload;
	int* r1_payload = (int*)(*r1)->payload;
	int* r2_payload = (int*)(*r2)->payload;

	for (size_t i = 0; i < pos->num_tuples; i++) {
		int idx = hash(val_payload[i], htable->max_size);
		hash_entry* current = htable->table[idx];

		while(current) {
			if(current->key == val_payload[i]) {
				r1_payload[(*r1)->num_tuples++] = pos_payload[i];
				r2_payload[(*r2)->num_tuples++] = current->value;
			}
			current = current->next;
		}
	}

	s.code = OK;
	return s;
}