#ifndef LINKED_PAGE_LIST_TUPLE_DEFINITIONS
#define LINKED_PAGE_LIST_TUPLE_DEFINITIONS

#include<tuple.h>
#include<inttypes.h>

#include<page_access_specification.h>

typedef struct linked_page_list_tuple_defs linked_page_list_tuple_defs;
struct linked_page_list_tuple_defs
{
	// specification of all the pages in the linked_page_list
	const page_access_specs* pas_p;

	// tuple definition of all the pages in the linked_page_list
	tuple_def* record_def;

	// maximum size of the record, that can be inserted into this linked_page_list
	uint32_t max_record_size;
};

// initializes the attributes in linked_page_list_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it allocates memory only for record_def
// returns 1 for success, it fails with 0
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
// it also fails if all of the record_def' records does not fit on the page
int init_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p, const page_access_specs* pas_p, const tuple_def* record_def);

// it deallocates the record_def and
// then resets all the page_table_tuple_defs struct attributes to NULL or 0
void deinit_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p);

// print linked_page_list_tuple_defs
void print_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p);

#endif