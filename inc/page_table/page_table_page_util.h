#ifndef PAGE_TABLE_PAGE_UTIL_H
#define PAGE_TABLE_PAGE_UTIL_H

#include<page_table_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page_functions.h>

// initialize page table page
int init_page_table_page(persistent_page* ppage, uint32_t level, uint64_t first_bucket_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// ptints page table page
int print_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

// NOTE:: the below 3 functions allow you to access a page_table_page as an array of page_ids of size = pttd_p->entries_per_page

// get child_page_id in the page at child_index
uint64_t get_child_page_id_at_child_index_in_page_table_page(const persistent_page* ppage, uint32_t child_index, const page_table_tuple_defs* pttd_p);

// set child_page_id in the page at child_index
// fails if child_index > pttd_p->entries_per_page
int set_child_page_id_at_child_index_in_page_table_page(persistent_page* ppage, uint32_t child_index, uint64_t child_page_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns true if all the child pointers in the page_table_page are NULL_PAGE_ID
// i.e. tombstones_count == tuples_count
int has_all_NULL_PAGE_ID_in_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

// NOTE:: the above 3 functions allow you to access a page_table_page as an array of page_ids of size = pttd_p->entries_per_page

#endif