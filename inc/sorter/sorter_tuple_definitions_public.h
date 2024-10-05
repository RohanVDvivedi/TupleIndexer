#ifndef SORTER_TUPLE_DEFINITIONS_PUBLIC_H
#define SORTER_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuple.h>
#include<inttypes.h>

#include<linked_page_list_tuple_definitions_public.h>
#include<page_table_tuple_definitions_public.h>

typedef struct sorter_tuple_defs sorter_tuple_defs;
struct sorter_tuple_defs
{
	// number of elements considered as keys, to sort on
	uint32_t key_element_count;

	// element ids of the keys (as per their element_ids in record_def) in the order as you want them to be ordered
	const positional_accessor* key_element_ids;

	// compare direction for the keys, array of ASC/DESC
	const compare_direction* key_compare_direction;

	// tuple definition of the records
	const tuple_def* record_def;

	// tuple_definiton for the runs of the sorter
	linked_page_list_tuple_defs lpltd;

	// tuple_definition for the pointers to the runs of the sorter
	page_table_tuple_defs pttd;
};

// initializes the attributes in sorter_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// returns 1 for success, it fails with 0, if the record_def has element_count 0 OR key_element_count == 0 OR key_element_ids == NULL OR if any of the key_element_ids is out of bounds
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_sorter_tuple_definitions(sorter_tuple_defs* std_p, const page_access_specs* pas_p, const tuple_def* record_def, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count);

// checks to see if a record_tuple can be inserted into the sorter
// note :: you can not insert a NULL record in bplus_tree
int check_if_record_can_be_inserted_for_sorter_tuple_definitions(const sorter_tuple_defs* std_p, const void* record_tuple);

// it deallocates the key_element_ids, key_compare_direction, record_def, lpltd and pttd
// then resets all the sorter_tuple_defs struct attributes to NULL or 0
void deinit_sorter_tuple_definitions(sorter_tuple_defs* std_p);

// print bplus_tree_tuple_definitions
void print_sorter_tuple_definitions(const sorter_tuple_defs* std_p);

#endif