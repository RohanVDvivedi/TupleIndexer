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

typedef struct bplus_tree_read_cursor bplus_tree_read_cursor;
struct bplus_tree_read_cursor
{
	// the page that is locked for reading
	void* read_page;

	// index of the record that is yet to be read
	uint32_t record_id;
};

int create_new_bplus_tree(bplus_tree_handle* bpth, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);


// find function uses a read cursor
// returns 1 if an exact match was found
// read cursor can be NULL, this allows find to return 1, if the record with given key exists
int find_in_bplus_tree(bplus_tree_handle* bpth, const void* key, bplus_tree_read_cursor* rc, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

// seek returns 1, if the seek was success full
// it fails with 0, if we reached the end of b+tree
int seek_next_read_cursor(bplus_tree_read_cursor* rc, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

// returns pointer to the record that the read cursor is pointing to
const void* get_record_from_read_cursor(bplus_tree_read_cursor* rc, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

// you must call this method to release the locked page by the read cursor
void close_read_cursor(bplus_tree_read_cursor* rc, const data_access_methods* dam_p);


// point insert
int insert_in_bplus_tree(bplus_tree_handle* bpth, const void* record, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

// point delete
int delete_in_bplus_tree(bplus_tree_handle* bpth, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);


// print complete b+ tree
void print_bplus_tree(bplus_tree_handle* bpth, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

#endif