#ifndef STORAGE_CAPACITY_PAGE_UTIL_H
#define STORAGE_CAPACITY_PAGE_UTIL_H

#include<tupleindexer/utils/persistent_page.h>
#include<tuplestore/tuple_def.h>

// The below functions only read the page provided and must not and will not modify the page

int is_page_lesser_than_half_full(const persistent_page* ppage, uint32_t page_size, const tuple_def* def);

int is_page_lesser_than_or_equal_to_half_full(const persistent_page* ppage, uint32_t page_size, const tuple_def* def);

int is_page_more_than_or_equal_to_half_full(const persistent_page* ppage, uint32_t page_size, const tuple_def* def);

int is_page_more_than_half_full(const persistent_page* ppage, uint32_t page_size, const tuple_def* def);

// method only valid for bplus tree and variants
// returns 1 -> MAY require split
// returns 0 -> will SURELY not require any split
// it may give false positive for splitting
int may_require_split_for_insert_for_bplus_tree(const persistent_page* ppage, uint32_t page_size, const tuple_def* def);

// method only valid for bplus tree and variants
// returns 1 -> MAY require merge or redistribution
// returns 0 -> will SURELY not require any merge or redistribution
// it may give false positive for merging or redistribution
// child_index is the index in the page, that we would be following
// child_index = -1 when you are following the least_keys_page_id
int may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(const persistent_page* ppage, uint32_t page_size, const tuple_def* index_def, uint32_t child_index);

#endif