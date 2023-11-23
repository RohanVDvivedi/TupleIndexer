#ifndef PAGE_TABLE_PAGE_UTIL_H
#define PAGE_TABLE_PAGE_UTIL_H

#include<page_table_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page_functions.h>

// initialize page table page
void init_page_table_page(persistent_page* ppage, uint32_t level, uint64_t first_bucket_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// get child_page_id in the page at child_index
uint64_t get_child_page_id_at_child_index(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

// set child_page_id in the page at child_index
int set_child_page_id_at_child_index(const persistent_page* ppage, uint64_t child_page_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif