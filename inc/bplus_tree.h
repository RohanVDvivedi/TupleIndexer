#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include<bplus_tree_cursor.h>

#include<data_access_methods.h>
#include<bplus_tree_tuple_definitions.h>

// returns pointer to the root page of the bplus_tree
uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// returns a bplus_tree_cursor to read from key (key inclusive)
// if record with given key is absent from the bplus_tree 
// 	then if scan_dir == -1, then the cursor points to a record lesser than the key
// 	then if scan_dir == +1, then the cursor points to a record greater than the key
// if the key is NULL
// 	then if the scan_dir == -1, then the cursor points to the greatest record in the bplus_tree
// 	then if the scan_dir == +1, then the cursor points to the least record in the bplus_tree
bplus_tree_cursor* find_in_bplus_tree(uint64_t root_page_id, const void* key, int scan_dir, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// insert record in bplus_tree
int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// delete a record given by key
int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// frees all the pages occupied by the bplus_tree recursively
int destroy_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// prints all the pages in the bplus_tree recursively
void print_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

#endif