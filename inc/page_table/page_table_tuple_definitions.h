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

	// power table for entries_per_page
	// ith value in this row = entries_per_page ^ (2 ^ i)
	// if the i-1 th value is n, then the ith value is n^2
	uint64_t power_table[64];

	// for all indices greater than this value, the power_table is useless, because it has overflowed
	uint8_t power_table_overflows_at;

	// the maximum height of the page table will never be more than this value
	uint64_t max_page_table_height;
};

// initializes the attributes in page_table_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it allocates memory only for entry_def
// returns 1 for success, it fails with 0
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p);

// if return == 1, after the call to this function, result will be set to entries_per_page ^ exp
// on an overflow return == 0
int get_power_of_entries_per_page(const page_table_tuple_defs* pttd_p, uint64_t exp, uint64_t* result);

// it deallocates the entry_def and
// then resets all the page_table_tuple_defs struct attributes to NULL or 0
void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p);

// print page_table_tree_tuple_defs
void print_page_table_tuple_definitions(page_table_tuple_defs* pttd_p);

#endif