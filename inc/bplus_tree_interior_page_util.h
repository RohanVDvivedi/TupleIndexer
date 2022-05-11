#ifndef BPLUS_TREE_INTERIOR_PAGE_UTIL_H
#define BPLUS_TREE_INTERIOR_PAGE_UTIL_H

#include<stdint.h>

#include<bplus_tree_interior_page_header.h>

#include<data_access_methods.h>

/*
*	child index start from (-1) and end at (tuple_count - 1)
*	child page_id at index = (-1) => is same as the "least_keys_page_id" as given in the bplus_tree_interior_page_header
*	all other child page_id are stored as the last attributes of the corresponding tuples
*/

int init_bplus_tree_interior_page(void* page, uint32_t level, const bplus_tree_tuple_defs* bpttd_p);

// this the index of the tuple in the interior page that you should follow
// you may cache this, it may help in case of a split
uint32_t find_child_index_for_key(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

// returns the page_id stored with the corresponding tuple at index, in its attribute "child_page_id" 
uint64_t find_child_page_id_by_child_index(const void* page, uint32_t index, const bplus_tree_tuple_defs* bpttd_p);

// it performs a split insert to the interior page provided
// and returns the tuple that needs to be inserted to the parent page
// you may call this function only if you are sure that the new_tuple will not fit on the page even after a compaction
// it returns NULL, on failure if the tuple was not inserted, and the split was not performed
// This function MUST be called only if the direct insert (OR a compaction + insert) to this page fails, (else it will fail the split regardless)
// lock on page1 is not released, all other pages locked in the scope of this function are unlocked in the same scope
const void* split_insert_interior_page(void* page1, uint64_t page1_id, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, data_access_methods* dam_p);

#endif