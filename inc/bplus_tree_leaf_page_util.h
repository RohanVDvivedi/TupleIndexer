#ifndef BPLUS_TREE_LEAF_PAGE_UTIL_H
#define BPLUS_TREE_LEAF_PAGE_UTIL_H

#include<stdint.h>

#include<bplus_tree_interior_page_header.h>
#include<data_access_methods.h>

int init_bplus_tree_leaf_page(void* page, const bplus_tree_tuple_defs* bpttd_p);

// when you want tuples in descending order from the key
uint32_t find_greater_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

// when you want tuples in ascending order from the key
uint32_t find_lesser_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p);

// it performs a split insert to the leaf page provided
// and returns the tuple that needs to be inserted to the parent page
// you may call this function only if you are sure that the new_tuple will not fit on the page even after a compaction
// it returns NULL, on failure if the tuple was not inserted, and the split was not performed
// This function MUST be called only if the direct insert (OR a compaction + insert) to this page fails, (else it will fail the split regardless)
// the failure may also result from following reason:
// failure to allocate a new page OR failure to get reference to the next page of the page1
// lock on page1 is not released, all other pages locked in the scope of this function are unlocked in the same scope
const void* split_insert_bplus_tree_leaf_page(void* page1, uint64_t page1_id, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p);

// it performs merge of the 2 leaf pages (page1 and the one next to it)
// the page1 must have an adjacent page and both of them must have a single parent node
// this function will perform page compaction on the page1 if required
// it fails with a 0 if the pages can not be merged (this may be due to their used spaces greater than the allotted size on the page1)
// lock on page1 is not released, all other pages locked in the scope of this function are unlocked in the same scope
// if this function returns a 1, then it is left on to the calling function to delete the corresponding parent entry of the page that is next to page1
// lock on page1 is not released, all other pages locked in the scope of this function are unlocked in the same scope
int merge_bplus_tree_leaf_pages(void* page1, uint64_t page1_id, bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p);

#endif