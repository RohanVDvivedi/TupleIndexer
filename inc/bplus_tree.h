#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include<bplus_tree_cursor.h>

#include<data_access_methods.h>
#include<bplus_tree_tuple_definitions.h>

// returns pointer to the root page of the bplus_tree
uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// returns a bplus_tree_cursor to read from key_start to key_last both inclusive
// the direction of the cursor is decided on the basis of ordering of key_start and key_last
bplus_tree_cursor* find_range_in_bplus_tree(uint64_t root_page_id, const void* key_start, const void* key_last, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// insert record in bplus_tree
int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// delete a record given by key
int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// frees all the pages occupied by the bplus_tree recursively
int destroy_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// prints all the pages in the bplus_tree recursively
void print_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

#endif