#ifndef PAGE_TABLE_H
#define PAGE_TABLE_H

#include<page_table_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<opaque_page_access_methods.h>

// returns pointer to the root page of the newly created page_table
uint64_t get_new_page_table(const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the page_table
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given page_table
int destroy_page_table(uint64_t root_page_id, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the pages in the page_table
// it may return an abort_error, unable to print all of the page_table pages
void print_page_table(uint64_t root_page_id, int only_leaf_pages, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif