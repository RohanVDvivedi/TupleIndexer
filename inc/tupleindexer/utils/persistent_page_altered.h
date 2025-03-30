#ifndef PERSISTENT_PAGE_ALTERED_H
#define PERSISTENT_PAGE_ALTERED_H

#include<tupleindexer/utils/persistent_page_functions.h>

#include<tuplestore/tuple.h>

int init_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d, int* abort_error);

void set_persistent_page_header(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const void* hdr, int* abort_error);

int append_tuple_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error);

int insert_tuple_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error);

int update_tuple_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error);

int discard_tuple_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, int* abort_error);

void discard_all_tuples_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error);

uint32_t discard_trailing_tomb_stones_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error);

int swap_tuples_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2, int* abort_error);

int set_element_in_tuple_in_place_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, positional_accessor element_index, const user_value* value, int* abort_error);

void clone_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const persistent_page* ppage_src, int* abort_error);

int run_persistent_page_compaction(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error);

#endif