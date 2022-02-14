#ifndef STORAGE_CAPACITY_PAGE_UTIL_H
#define STORAGE_CAPACITY_PAGE_UTIL_H

#include<tuple_def.h>

int is_page_lesser_than_half_full(const void* page, uint32_t page_size, const tuple_def* def);

int is_page_lesser_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def);

int is_page_more_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def);

int is_page_more_than_half_full(const void* page, uint32_t page_size, const tuple_def* def);

// method only valid for bplus tree and variants
// returns 1 -> MAY require split
// returns 0 -> will SURELY not require any split
// it may give false positive for splitting
int may_split_on_insert_for_bplus_tree(const void* page, uint32_t page_size, const tuple_def* def);

#endif