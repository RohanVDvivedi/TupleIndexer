#ifndef PERSISTENT_PAGE_ALTERED_H
#define PERSISTENT_PAGE_ALTERED_H

#include<persistent_page_functions.h>

#include<tuple_def.h>

int init_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d);

void set_persistent_page_header(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const void* hdr);

int append_tuple_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple);

int update_tuple_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

int discard_tuple_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index);

void discard_all_tuples_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t discard_trailing_tomb_stones_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

int swap_tuples_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2);

int set_element_in_tuple_in_place_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, uint32_t element_index, const user_value* value);

void clone_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page* ppage_src);

int run_persistent_page_compaction(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

#endif