#ifndef PAGE_TABLE_TUPLE_DEFINITIONS_PUBLIC_H
#define PAGE_TABLE_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuple.h>
#include<inttypes.h>

#include<array_table_tuple_definitions_public.h>

// The function (find_non_NULL_PAGE_ID_in_page_table) makes use of the fact that entries_per_page != INVALID_INDEX (UINT32_MAX)
// i.e. entries_per_page < INVALID_INDEX (UINT32_MAX)

typedef struct page_table_tuple_defs page_table_tuple_defs;
struct page_table_tuple_defs
{
	// specification of all the pages in the page_table
	// this is redundant parameter, as it is duplicated in attd
	const page_access_specs* pas_p;

	// page table is internally a array_table with entries being page_ids
	array_table_tuple_defs attd;
};

// initializes the attributes in page_table_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it allocates memory only for entry_def
// returns 1 for success, it fails with 0
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p);

// it deallocates the entry_def and
// then resets all the page_table_tuple_defs struct attributes to NULL or 0
void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p);

// print page_table_tree_tuple_defs
void print_page_table_tuple_definitions(const page_table_tuple_defs* pttd_p);

#endif