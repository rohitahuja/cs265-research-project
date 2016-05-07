#ifndef BPT_H__
#define BPT_H__

/*
* The following B+ Tree Implementation is adapted from here:
* http://www.amittai.com/prose/bpt.c
*/
#include <stdio.h>
#include "cs165_api.h"
#include "helpers.h"
#include "utils.h"

#define DEFAULT_ORDER 4096

// TYPES.

typedef struct node {
	void** pointers;
	int* keys;
	struct node* parent;
	bool is_leaf;
	int num_keys; // keep track of the number of valid keys (num of valid pointers in a leaf)
	struct node* next; // Used for queue.
} node;

// FUNCTION PROTOTYPES.

// For utility
void print_leaves(node* root);
void print_tree(node* root);

// For find
node* find_leaf(node * root, int key);
int* find_in_bptree(node* root, int key);
status find_range_bpt(column* col, result* r, int lower, int upper);

// For insert
int cut(int length);
status make_node(node** new_node);
status make_leaf(node** leaf);
int get_left_index(node* parent, node* left);
status insert_into_leaf(node* leaf, int key, int* pointer);
status insert_into_leaf_after_splitting(node** root, node* leaf, int key, int* pointer);
status insert_into_node(node* n, int left_index, int key, node* right);
status insert_into_node_after_splitting(node** root, node* old_node, int left_index, int key, node* right);
status insert_into_parent(node** root, node* left, int key, node* right);
status insert_into_new_root(node** root, node* left, int key, node* right);
status start_new_tree(node** root, int key, int* ptr);
status insert_bpt(node** root, int key, int* value);
status update_bpt_pointers(node* root, int* pointer);
status build_secondary_bpt_index(column *col);

#endif // BPT_H__

