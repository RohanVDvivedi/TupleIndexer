#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H

#include<stdint.h>

#include<tuple.h>
#include<tuple_def.h>
#include<page_layout.h>

#include<data_access_methods.h>

uint32_t create_new_bplus_tree(const data_access_methods* dam_p);

const void* search_in_bplus_tree(const data_access_methods* dam_p);

insert_in_bpus_tree(const data_access_methods* dam_p);


#endif