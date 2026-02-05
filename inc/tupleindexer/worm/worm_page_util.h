#ifndef WORM_PAGE_UTIL_H
#define WORM_PAGE_UTIL_H

#include<stdint.h>

#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/worm/worm_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

// initializes a new worm head page, with tail_page_id = ppage.page_id (we assume this is the first page being initialized for creation of a worm, so it itself is tha tail)
// and next_page_id = NULL_PAGE_ID
// reference_counter (logically must be non zero, initially), dependent_root (may be NULL_PAGE_ID)
int init_worm_head_page(persistent_page* ppage, uint32_t reference_counter, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// initializes a new any worm page, with next_page_id = NULL_PAGE_ID
int init_worm_any_page(persistent_page* ppage, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

// if this function returns 0, there is no more any space on this page, try adding a new tail page
// else attempt to insert a binary with at most this size and it will succeed in being appended on the worm page
uint32_t binary_bytes_appendable_on_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

// utility function to update next_page_id on the ppage
int update_next_page_id_on_worm_page(persistent_page* ppage, uint64_t next_page_id, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif