#include "bpt.h"
#include "helpers.h"

// GLOBALS.

int order = DEFAULT_ORDER;

node* queue = NULL;

// UTILITY FUNCTIONS
void print_leaves(node * root) {
	int i;
	node * c = root;
	if (root == NULL) {
		printf("Empty tree.\n");
		return;
	}
	while (!c->is_leaf)
		c = c->pointers[0];
	while (true) {
		for (i = 0; i < c->num_keys; i++) {
			printf("%d ", c->keys[i]);
		}
		if (c->pointers[order - 1] != NULL) {
			printf(" | ");
			c = c->pointers[order - 1];
		}
		else
			break;
	}
	printf("\n");
}

void enqueue( node * new_node ) {
	node * c;
	if (queue == NULL) {
		queue = new_node;
		queue->next = NULL;
	}
	else {
		c = queue;
		while(c->next != NULL) {
			c = c->next;
		}
		c->next = new_node;
		new_node->next = NULL;
	}
}


node * dequeue( void ) {
	node * n = queue;
	queue = queue->next;
	n->next = NULL;
	return n;
}

int path_to_root( node * root, node * child ) {
	int length = 0;
	node * c = child;
	while (c != root) {
		c = c->parent;
		length++;
	}
	return length;
}

void print_tree( node * root ) {

	node * n = NULL;
	int i = 0;
	int rank = 0;
	int new_rank = 0;

	if (root == NULL) {
		printf("Empty tree.\n");
		return;
	}
	queue = NULL;
	enqueue(root);
	while( queue != NULL ) {
		n = dequeue();
		if (n->parent != NULL && n == n->parent->pointers[0]) {
			new_rank = path_to_root( root, n );
			if (new_rank != rank) {
				rank = new_rank;
				printf("\n");
			}
		}
		for (i = 0; i < n->num_keys; i++) {
			printf("%d ", n->keys[i]);
		}
		if (!n->is_leaf) {
			for (i = 0; i <= n->num_keys; i++) {
				enqueue(n->pointers[i]);
			}
		}
		printf("| ");
	}
	printf("\n");
}

node* find_leaf(node* root, int key) {
	int i = 0;
	node * c = root;
	if (c == NULL) {
		return c;
	}
	while (!c->is_leaf) {
		i = binary_search(c->keys, key, 0, c->num_keys);
		if (i != 0) {
			while(c->keys[i] != key && i < c->num_keys-1 && c->keys[i] == c->keys[i+1]) {
				i++;
			}
		}

		c = (node *)c->pointers[i];
	}

	return c;
}

int* find_in_bptree(node* root, int key) {
	int i = 0;
	node* c = find_leaf(root, key);
	if (c == NULL) {
		return NULL;
	}
	for (i = 0; i < c->num_keys; i++) {
		if (c->keys[i] == key) {
			break;	
		}
	}
	if (i == c->num_keys) {
		return NULL;
	}
	else {
		return (int*)c->pointers[i];
	}
}

status find_range_bpt_leading(column* col, result* r, int lower, int upper) {
	log_info("Searching in BPT on leading column\n");
	status s;

	int* payload = (int*)r->payload;
	int* start_addr = col->data;
	int i, j, k;
	int* ptr;

	node* root = (node*)col->index->index;
	node* start = find_leaf(root, lower);
	node* end = find_leaf(root, upper);

	if (start == NULL || end == NULL) {
		s.code = ERROR;
		s.error_message = "Can't find val\n";
		return s;
	}

	i = binary_search(start->keys, lower, 0, start->num_keys);
	j = binary_search(end->keys, upper, 0, end->num_keys);
	
	while(i == start->num_keys) {
		start = start->pointers[order - 1];
		if (!start) break;
		i = binary_search(start->keys, lower, 0, start->num_keys);
	}

	while(j == end->num_keys) {
		end = end->pointers[order - 1];
		if (!end) break;
		j = binary_search(end->keys, upper, 0, end->num_keys);
	}

	if (start == NULL || end == NULL) {
		s.code = ERROR;
		s.error_message = "Can't find val\n";
		return s;
	}

	for(k = 0, ptr = (int*)start->pointers[i]; ptr < (int*)end->pointers[j]; k++, ptr++) {
		payload[k] = ptr - start_addr;
	}

	r->type = INT;
	r->num_tuples = k;

	s.code = OK;
	return s;
}

status find_range_bpt(column* col, result* r, int lower, int upper) {
	log_info("Searching in BPT\n");

	if (col->leading) {
		return find_range_bpt_leading(col, r, lower, upper);
	}

	status s;

	int* payload = (int*)r->payload;
	int* start_addr = col->data;
	int i, j;

	node* root = (node*)col->index->index;
	node* n = find_leaf(root, lower);
	if (n == NULL) {
		s.code = ERROR;
		s.error_message = "Can't find val\n";
		return s;
	}

	i = binary_search(n->keys, lower, 0, n->num_keys);
	while(i == n->num_keys) {
		n = n->pointers[order - 1];
		if (!n) break;
		i = binary_search(n->keys, lower, 0, n->num_keys);
	}

	if (n == NULL) {
		s.code = ERROR;
		s.error_message = "Can't find val\n";
		return s;
	}

	j = 0;
	while(n != NULL) {
		for(; i < n->num_keys && n->keys[i] <= upper; i++) {
			payload[j] = (int*)n->pointers[i] - start_addr;
			j++;
		}
		n = n->pointers[order - 1];
		i = 0;
	}
	r->type = INT;
	r->num_tuples = j;

	s.code = OK;
	return s;
}

status make_node(node** new_node) {
	status s;

	*new_node = malloc(sizeof(node));
	if (*new_node == NULL) {
		s.code = ERROR;
		s.error_message = "Error creating node\n";
		return s;
	}
	(*new_node)->keys = malloc( (order - 1) * sizeof(int) );
	if ((*new_node)->keys == NULL) {
		s.code = ERROR;
		s.error_message = "Error creating new node keys array\n";
		return s;
	}
	(*new_node)->pointers = malloc( order * sizeof(void *) );
	if ((*new_node)->pointers == NULL) {
		s.code = ERROR;
		s.error_message = "Error creating new node pointers array\n";
		return s;
	}
	(*new_node)->is_leaf = false;
	(*new_node)->num_keys = 0;
	(*new_node)->parent = NULL;
	(*new_node)->next = NULL;

	s.code = OK;
	return s;
}

status make_leaf(node** leaf) {
	status s = make_node(leaf);
	if (s.code != OK) {
		return s;
	}
	(*leaf)->is_leaf = true;

	return s;
}

status insert_into_new_root(node** root, node* left, int key, node* right) {
	status s = make_node(root);
	if (s.code != OK) {
		return s;
	}

	(*root)->keys[0] = key;
	(*root)->pointers[0] = left;
	(*root)->pointers[1] = right;
	(*root)->num_keys++;
	(*root)->parent = NULL;
	left->parent = (*root);
	right->parent = (*root);

	return s;
}

int cut(int length) {
	return length/2 + (length % 2);
}

status insert_into_node_after_splitting(node** root, node* old_node, int left_index, int key, node* right) {
	status s;
	int i, j, split, k_prime;
	node* new_node, *child;
	int* temp_keys;
	node** temp_pointers;

	temp_pointers = malloc( (order + 1) * sizeof(node *) );
	if (temp_pointers == NULL) {
		s.code = ERROR;
		s.error_message = "Error creating temporary pointers array for splitting nodes\n";
		return s;
	}

	temp_keys = malloc( order * sizeof(int) );
	if (temp_keys == NULL) {
		s.code = ERROR;
		s.error_message = "Error creating temporary keys array for splitting nodes\n";
		return s;
	}

	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1) j++;
		temp_pointers[j] = old_node->pointers[i];
	}

	for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index) j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;

	split = cut(order);

	new_node = NULL;
	s = make_node(&new_node);
	if (s.code != OK) {
		return s;
	}

	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];

	k_prime = temp_keys[split - 1];

	for (++i, j = 0; i < order; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->num_keys++;
	}
	new_node->pointers[j] = temp_pointers[i];

	free(temp_pointers);
	free(temp_keys);

	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->num_keys; i++) {
		child = new_node->pointers[i];
		child->parent = new_node;
	}

	return insert_into_parent(root, old_node, k_prime, new_node);
}

int get_left_index(node * parent, node * left) {

	int left_index = 0;
	while (left_index <= parent->num_keys && 
			parent->pointers[left_index] != left)
		left_index++;
	return left_index;
}

status insert_into_parent(node** root, node* left, int key, node* right) {
	int left_index;
	node* parent;

	parent = left->parent;


	if (parent == NULL) {
		return insert_into_new_root(root, left, key, right);
	}

	left_index = get_left_index(parent, left);

	if (parent->num_keys < order - 1) {
		return insert_into_node(parent, left_index, key, right);
	}

	return insert_into_node_after_splitting(root, parent, left_index, key, right);
}

status insert_into_leaf(node* leaf, int key, int* pointer) {
	status s;
	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
		insertion_point++;

	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pointers[i] = leaf->pointers[i - 1];
	}
	leaf->keys[insertion_point] = key;
	leaf->pointers[insertion_point] = pointer;
	leaf->num_keys++;

	s.code = OK;
	return s;
}

status insert_into_leaf_after_splitting(node** root, node* leaf, int key, int* pointer) {
	status s;
	node* new_leaf;
	int* temp_keys;
	void** temp_pointers;
	int insertion_index, split, new_key, i, j;

	new_leaf = NULL;
	s = make_leaf(&new_leaf);
	if (s.code != OK) {
		return s;
	}

	// Allocate temp arrays to represent full node ordering prior to split
	temp_keys = malloc(order*sizeof(int));
	if (temp_keys == NULL) {
		s.code = ERROR;
		s.error_message = "Error creating temporary keys array\n";
		return s;
	}

	temp_pointers = malloc(order*sizeof(void *));
	if (temp_pointers == NULL) {
		s.code = ERROR;
		s.error_message = "Error creating temporary pointers array\n";
		return s;
	}

	// Figure out insertion index and copy into temp arrays valid ordering
	insertion_index = 0;
	while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
		insertion_index++;

	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index) j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->pointers[i];
	}

	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = pointer;

	leaf->num_keys = 0;

	// Figure out split point
	split = cut(order - 1);

	// Copy 0..split from temp to old leaf
	for (i = 0; i < split; i++) {
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	// Copy split..end from temp to new leaf
	for (i = split, j = 0; i < order; i++, j++) {
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}

	// Free my homie temps
	free(temp_pointers);
	free(temp_keys);

	// Set right pointer of old leaf as right pointer of new_leaf
	new_leaf->pointers[order - 1] = leaf->pointers[order - 1];

	// Set right pointer of old leaf as new_leaf
	leaf->pointers[order - 1] = new_leaf;

	// NULL out remainder of old leaf arrays
	for (i = leaf->num_keys; i < order - 1; i++)
		leaf->pointers[i] = NULL;

	// NULL out remainder of new_leaf arrays
	for (i = new_leaf->num_keys; i < order - 1; i++)
		new_leaf->pointers[i] = NULL;

	// Set leaf parent the same for both
	new_leaf->parent = leaf->parent;

	// Grab leftmost key of new_leaf and propogate to the parent
	new_key = new_leaf->keys[0];

	return insert_into_parent(root, leaf, new_key, new_leaf);
}

status insert_into_node(node* n, int left_index, int key, node* right) {
	status s;
	int i;

	for (i = n->num_keys; i > left_index; i--) {
		n->pointers[i + 1] = n->pointers[i];
		n->keys[i] = n->keys[i - 1];
	}
	n->pointers[left_index + 1] = right;
	n->keys[left_index] = key;
	n->num_keys++;

	s.code = OK;
	return s;
}

status start_new_tree(node** root, int key, int* ptr) {
	status s = make_leaf(root);
	if (s.code != OK) {
		return s;
	}

	(*root)->keys[0] = key;
	(*root)->pointers[0] = ptr;
	(*root)->pointers[order - 1] = NULL;
	(*root)->parent = NULL;
	(*root)->num_keys++;
	
	return s;
}

status insert_bpt(node** root, int key, int* value) {
	log_info("Inserting in BPT\n");
	status s;
	node * leaf;

	if (*root == NULL) {
		return start_new_tree(root, key, value);
	}

	leaf = find_leaf(*root, key);

	if (leaf->num_keys < order - 1) {
		s = insert_into_leaf(leaf, key, value);
		return s;
	}

	s = insert_into_leaf_after_splitting(root, leaf, key, value);
	
	return s;
}

status build_secondary_bpt_index(column *col) {
    status s;

    node** root = (node**) (&(col->index->index));
    for (size_t i = 0; i < col->data_count; i++) {
        s = insert_bpt(root, col->data[i], &(col->data[i]));
        if (s.code != OK) {
            return s;
        }
    }

    s.code = OK;
    return s;
}