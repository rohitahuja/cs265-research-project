#ifndef JOIN_H__
#define JOIN_H__

#include <stdio.h>
#include "cs165_api.h"

typedef struct hash_entry {
	int key;
	int value;
	struct hash_entry* next;
} hash_entry;

typedef struct hash_table {
	size_t current_size;
	size_t max_size;
	struct hash_entry** table;
} hash_table;

typedef struct partition {
	int level;
	int* val1;
	int* pos1;
	int* val2;
	int* pos2;
	size_t num_tuples1;
	size_t num_tuples2;
	struct partition* c1;
	struct partition* c2;
} partition;

status join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2);
status hash_join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2);
hash_table* hash_table_init (size_t htable_size);
hash_entry* hash_entry_init (int key, int value);
status insert_hash_table(hash_table* htable, result* pos, result* val);
status entry_insert(hash_entry** head, hash_entry* entry);
status probe(hash_table* htable, result* pos, result* val, result** r1, result** r2);
status nested_join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2);
partition* init_partition(int level, size_t num_tuples1, size_t num_tuples2);
partition* init_root(result* pos1, result* val1, result* pos2, result* val2);
status partition_node(partition* node);
status join_partitions(partition* node, result** r1, result** r2);
status partition_hash_join(result* pos1, result* val1, result* pos2, result* val2, result** r1, result** r2);

#endif // JOIN_H__