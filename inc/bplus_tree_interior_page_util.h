#ifndef BPLUS_TREE_INTERIOR_PAGE_UTIL_H
#define BPLUS_TREE_INTERIOR_PAGE_UTIL_H

#include<stdint.h>

#include<persistent_page.h>
#include<bplus_tree_tuple_definitions.h>
#include<opaque_data_access_methods.h>
#include<opaque_page_modification_methods.h>

/*
*	child index start from (-1) and end at (tuple_count - 1)
*	child page_id at index = (-1) => is same as the "least_keys_page_id" as given in the bplus_tree_interior_page_header
*	all other child page_id are stored as the last attributes of the corresponding tuples
*/

int init_bplus_tree_interior_page(persistent_page* ppage, uint32_t level, int is_last_page_of_level, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p);

// prints of bplus_tree interior page
void print_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

// this the index of the tuple in the interior page that you should follow
// you may cache this, it may help in case of a split
// the constrained parameter key_element_count_concerned <= bpttd_p->bpttd_p->key_element_count
// allows you to consider lesser number of key columns for your operation
uint32_t find_child_index_for_key(const persistent_page* ppage, const void* key, uint32_t key_element_count_concerned, const bplus_tree_tuple_defs* bpttd_p);

// this the index of the tuple in the interior page that you should follow
// you may cache this, it may help in case of a split
// the constrained parameter key_element_count_concerned <= bpttd_p->bpttd_p->key_element_count
// allows you to consider lesser number of key columns for your operation
uint32_t find_child_index_for_record(const persistent_page* ppage, const void* record, uint32_t key_element_count_concerned, const bplus_tree_tuple_defs* bpttd_p);

// returns the page_id stored with the corresponding tuple at index, in its attribute "child_page_id" 
uint64_t get_child_page_id_by_child_index(const persistent_page* ppage, uint32_t index, const bplus_tree_tuple_defs* bpttd_p);

// check if a bplus tree interior page must split for an insertion of a tuple
int must_split_for_insert_bplus_tree_interior_page(const persistent_page* page1, const void* tuple_to_insert, const bplus_tree_tuple_defs* bpttd_p);

// it performs a split insert to the interior page provided
// and returns the tuple that needs to be inserted to the parent page
// you may call this function only if you are sure that the new_tuple will not fit on the page even after a compaction
// it returns 0, on failure if the tuple was not inserted, and the split was not performed
// This function MUST be called only if the direct insert (OR a compaction + insert) to this page fails, (else it will fail the split regardless)
// lock on page1 is not released, all other pages locked in the scope of this function are unlocked in the same scope
int split_insert_bplus_tree_interior_page(persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, void* output_parent_insert);

// check if 2 bplus_tree interior pages can be merged
int can_merge_bplus_tree_interior_pages(const persistent_page* page1, const void* separator_parent_tuple, const persistent_page* page2, const bplus_tree_tuple_defs* bpttd_p);

// it performs merge of the 2 leaf pages (page1 and page2 the one next to it)
// the page1 must have an adjacent page and both of them must have a single parent node
// separator_tuple is the tuple at the child_index corresponding to page2 in the parent page
// this function will perform page compaction on the page1 if required
// it fails with a 0 if the pages can not be merged (this may be due to their used spaces greater than the allotted size on the page1)
// OR if the parameters passed are deemed invalid (like if page_pointer of separator_tuple is not page2_id)
// lock on page1 is not released, all other pages locked in the scope of this function are unlocked in the same scope
// page2 is not freed by the function, you MUST free this page if this function returns a 1 (stating a successful merge)
// if this function returns a 1, then separator_tuple must be deleted from the parent page
int merge_bplus_tree_interior_pages(persistent_page* page1, const void* separator_parent_tuple, persistent_page* page2, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p);

#endif