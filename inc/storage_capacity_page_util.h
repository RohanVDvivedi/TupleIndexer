#ifndef STORAGE_CAPACITY_PAGE_UTIL_H
#define STORAGE_CAPACITY_PAGE_UTIL_H

#include<tuple_def.h>

int is_page_lesser_than_half_full(const void* page, uint32_t page_size, const tuple_def* def);

int is_page_lesser_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def);

int is_page_more_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def);

int is_page_more_than_half_full(const void* page, uint32_t page_size, const tuple_def* def);

#endif