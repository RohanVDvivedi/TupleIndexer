#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H

#include<stdint.h>

#include<tuple.h>
#include<tuple_def.h>
#include<page_layout.h>

#include<data_access_methods.h>

#include<bplus_tree_util.h>

uint32_t create_new_bplus_tree(const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

const void* find_in_bplus_tree(uint32_t root, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

int insert_in_bplus_tree(uint32_t* root, const void* record, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

int delete_in_bplus_tree(uint32_t* root, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

#endif