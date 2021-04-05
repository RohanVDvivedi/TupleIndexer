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

uint32_t create_new_bplus_tree(const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

const void* find_in_bplus_tree(uint32_t root_id, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

int insert_in_bplus_tree(uint32_t* root_id, const void* record, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

int delete_in_bplus_tree(uint32_t* root_id, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

void print_bplus_tree(uint32_t root_id, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

#endif