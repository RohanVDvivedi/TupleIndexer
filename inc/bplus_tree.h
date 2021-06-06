#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H

#include<stdint.h>

#include<rwlock.h>

#include<tuple.h>
#include<tuple_def.h>
#include<page_layout.h>

#include<data_access_methods.h>
#include<page_offset_util.h>

#include<read_cursor.h>

typedef struct bplus_tree_tuple_defs bplus_tree_tuple_defs;
struct bplus_tree_tuple_defs
{
	// tuple definition to compare keys in the bplus_tree
	tuple_def* key_def;

	// tuple definition of the interior pages in the bplus_tree
	// this is equal to { key_def : page_id(u4)) }
	tuple_def* index_def;

	// tuple definition of the leaf pages in the bplus_tree
	// this is equal to { key_def : value_def }
	tuple_def* record_def;
};

typedef struct bplus_tree_handle bplus_tree_handle;
struct bplus_tree_handle
{
	// tuple definitions of the key, of index pages and of leaf pages of the bplus_tree
	// it remains constant throughout the life of the bplus_tree
	bplus_tree_tuple_defs tuple_definitions;

	// lock to protect only the root_id of the bplus tree
	rwlock handle_lock;

	// page id of the root page of the bplus tree
	uint32_t root_id;
};

// root_id == NULL_PAGE_REF for a new bplus_tree
int init_bplus_tree(bplus_tree_handle* bpth, const tuple_def* record_def, uint16_t key_element_count, uint32_t root_id);
int deinit_bplus_tree(bplus_tree_handle* bpth);

// find function uses a read cursor
// returns 1 if an exact match was found
// read cursor can be NULL, this allows find to return 1, if the record with given key exists
int find_in_bplus_tree(bplus_tree_handle* bpth, const void* key, read_cursor* rc, const data_access_methods* dam_p);

// point insert
int insert_in_bplus_tree(bplus_tree_handle* bpth, const void* record, const data_access_methods* dam_p);

// point delete
int delete_in_bplus_tree(bplus_tree_handle* bpth, const void* key, const data_access_methods* dam_p);


// print complete b+ tree
void print_bplus_tree(bplus_tree_handle* bpth, const data_access_methods* dam_p);

#endif