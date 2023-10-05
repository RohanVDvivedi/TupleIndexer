#ifndef PERSISTENT_PAGE_UNALTERED_H
#define PERSISTENT_PAGE_UNALTERED_H

#include<persistent_page.h>

#include<tuple_def.h>

// provides same functionality as page_layout_unaltered, but on a persistent_page

const void* get_page_header_ua_persistent_page(const persistent_page* ppage, uint32_t page_size);

uint32_t get_page_header_size_persistent_page(const persistent_page* ppage, uint32_t page_size);

uint32_t get_tuple_count_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t get_tomb_stone_count_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

int can_append_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple);

int can_update_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

int exists_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index);

const void* get_nth_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index);

uint32_t get_free_space_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t get_space_occupied_by_tuples_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t start_index, uint32_t last_index);

uint32_t get_space_occupied_by_all_tuples_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t get_space_occupied_by_all_tomb_stones_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t get_space_allotted_to_all_tuples_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t get_fragmentation_space_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t get_additional_space_overhead_per_tuple_on_persistent_page(uint32_t page_size, const tuple_size_def* tpl_sz_d);

void print_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_def* tpl_d);

#endif