#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H

#include<stdint.h>

#include<rwlock.h>

#include<tuple.h>
#include<tuple_def.h>
#include<page_layout.h>

#include<data_access_methods.h>

#include<bplus_tree_util.h>

typedef struct bplus_tree_handle bplus_tree_handle;
struct bplus_tree_handle
{
	// lock to protect root_id of the bplus tree
	rwlock handle_lock;

	// page id of the root page of the bplus tree
	uint32_t root_id;
};

int create_new_bplus_tree(bplus_tree_handle* bpth, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);


// find function uses a read cursor
// returns 1 if an exact match was found
// read cursor can be NULL, this allows find to return 1, if the record with given key exists
int find_in_bplus_tree(bplus_tree_handle* bpth, const void* key, read_cursor* rc, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

// point insert
int insert_in_bplus_tree(bplus_tree_handle* bpth, const void* record, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

// point delete
int delete_in_bplus_tree(bplus_tree_handle* bpth, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);


// print complete b+ tree
void print_bplus_tree(bplus_tree_handle* bpth, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

#endif