#ifndef ARRAY_TABLE_TUPLE_DEFINITIONS_PUBLIC_H
#define ARRAY_TABLE_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuple.h>
#include<inttypes.h>

#include<power_table.h>

#include<page_access_specification.h>

// The function (find_non_NULL_PAGE_ID_in_array_table) makes use of the fact that (leaf_ or index_)entries_per_page != INVALID_TUPLE_INDEX (UINT32_MAX)
// i.e. entries_per_page < INVALID_TUPLE_INDEX (UINT32_MAX)

typedef struct array_table_tuple_defs array_table_tuple_defs;
struct array_table_tuple_defs
{
	// specification of all the pages in the array_table
	const page_access_specs* pas_p;

	// tuple definition of all the pages in the array_table
	const tuple_def* record_def; // tuple definition of the leaf pages
	const tuple_def* index_def;  // tuple definition of interior pages -> this will be same as &(pas_p->page_id_tuple_def)
	// additionally each index_entry is just a UINT value of page_id_width in the entry_def

	// this decides the number of entries that can fit on any of the array_table page (leaf or interior pages)
	// this values are fixed since the record_def and index_def are both fixed sized
	uint64_t leaf_entries_per_page;		// can be any value between [1, UINT32_MAX)
	uint64_t index_entries_per_page;	// can be any value between [2, UINT32_MAX)

	// power table for index_entries_per_page
	power_table power_table_for_index_entries_per_page;

	// the maximum height of the array_table, it will never be more than this value
	uint64_t max_array_table_height;
};

// returns the maximum size of the fixed sized record of the fixed sized record_def that you can initialize the array_table for
uint32_t get_maximum_array_table_record_size(const page_access_specs* pas_p);

// initializes the attributes in array_table_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it allocates memory only for record_def and index_def
// returns 1 for success, it fails with 0
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
// it also fails if record_def provided is not fixed sized
int init_array_table_tuple_definitions(array_table_tuple_defs* attd_p, const page_access_specs* pas_p, const tuple_def* record_def);

// it deallocates both the record_def and index_def and
// then resets all the array_table_tuple_defs struct attributes to NULL or 0
void deinit_array_table_tuple_definitions(array_table_tuple_defs* attd_p);

// print array_table_tree_tuple_defs
void print_array_table_tuple_definitions(const array_table_tuple_defs* attd_p);

#endif