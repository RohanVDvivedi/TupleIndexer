#ifndef BPLUS_TREE_LEAF_PAGE_UTIL_H
#define BPLUS_TREE_LEAF_PAGE_UTIL_H

#include<stdint.h>

#include<bplus_tree_interior_page_header.h>
#include<bplus_tree_locked_pages_stack.h>

// when you want tuples in descending order from the key
uint32_t find_greater_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

// when you want tuples in ascending order from the key
uint32_t find_lesser_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

// it performs a split insert to the leaf page provided
// and returns the tuple that needs to be inserted to the parent page
// it returns NULL, on failure if the tuple was not inserted, and the split was not performed
// This function MUST be called only if the direct insert to this page fails, (else it will fail the split regardless)
// the failure results from following reason:
// failure to allocate a new page OR failure to get reference to the next page of the page_info
const void* split_insert_bplus_tree_leaf_page(locked_page_info* page_info, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p);

// it performs merge of the 2 leaf pages
// the page_info1 and page_info2 MUST be adjacent pages sharing a parent node
// this function will perform page compaction on the page_info1 if required
// it fails with a 0 if the pages can not be merged (this may be due to their used spaces greater than the allotted size on the page_info1)
// you may call this function regardless of you knowing that the pages may be merged
int merge_bplus_tree_leaf_pages(locked_page_info* page_info1, locked_page_info* page_info2, bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p);

#endif