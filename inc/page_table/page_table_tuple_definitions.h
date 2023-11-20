#ifndef PAGE_TABLE_TUPLE_DEFINITIONS_H
#define PAGE_TABLE_TUPLE_DEFINITIONS_H

#include<tuple.h>
#include<inttypes.h>

#include<page_access_specification.h>

typedef struct page_table_tuple_defs page_table_tuple_defs;
struct page_table_tuple_defs
{
	// specification of all the pages in the page_table
	const page_access_specs* pas_p;

	// tuple definition of all the pages in the page_table
	tuple_def* entry_def;
	// additionally each entry is just a UINT value of page_id_width in the entry_def

	// this decides the number of entries that can fit on any of the page_table page
	// this is fixed since the entry_def is fixed sized
	uint64_t entries_per_page;
};

// initializes the attributes in page_table_tuple_defs struct as per the provided parameters
// it allocates memory only for entry_def
// returns 1 for success, it fails with 0
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p);

// it deallocates the entry_def and
// then resets all the page_table_tuple_defs struct attributes to NULL or 0
void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p);

// print page_table_tree_tuple_defs
void print_page_table_tuple_definitions(page_table_tuple_defs* pttd_p);

#endif