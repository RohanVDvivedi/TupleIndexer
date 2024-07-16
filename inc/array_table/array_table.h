#ifndef ARRAY_TABLE_H
#define ARRAY_TABLE_H

#include<array_table_tuple_definitions_public.h>

#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

/*
	Array table is nothing but a look-up-table from uint64_t to record_def ofyour choosing
*/

// returns pointer to the root page of the newly created array_table
uint64_t get_new_array_table(const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the array_table
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given array_table
int destroy_array_table(uint64_t root_page_id, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the pages in the array_table
// it may return an abort_error, unable to print all of the array_table pages
void print_array_table(uint64_t root_page_id, int only_leaf_pages, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#include<array_table_range_locker_public.h>

#endif