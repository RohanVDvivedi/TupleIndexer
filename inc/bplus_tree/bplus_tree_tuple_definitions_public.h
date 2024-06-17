#ifndef BPLUS_TREE_TUPLE_DEFINITIONS_PUBLIC_H
#define BPLUS_TREE_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuple.h>
#include<inttypes.h>

#include<page_access_specification.h>

typedef struct bplus_tree_tuple_defs bplus_tree_tuple_defs;
struct bplus_tree_tuple_defs
{
	// specification of all the pages in the bplus_tree
	const page_access_specs* pas_p;

	// number of elements considered as keys
	uint32_t key_element_count;

	// element ids of the keys (as per their element_ids in record_def) in the order as you want them to be ordered
	uint32_t* key_element_ids;

	// compare direction for the keys, array of ASC/DESC
	compare_direction* key_compare_direction;

	// ith element_def in index_def has the same type, name and size as the key_element_ids[i] th element_def in record_def

	// tuple definition of the leaf pages in the bplus_tree
	tuple_def* record_def;

	// tuple definition of the interior pages in the bplus_tree
	tuple_def* index_def;

	// tuple definition of the key to be used with this bplus_tree
	// for all of find, insert, update and delete functionalities
	tuple_def* key_def;

	// precomputed value of max_record_size (for the tuple that goes into leaf pages of the bplus_tree) that can be inserted into a bplus_tree defined using this bplus_tree_tuple_defs struct
	uint32_t max_record_size;

	// precomputed value of max_index_record_size (for the tuple that goes into interior pages of the bplus_tree) that can be inserted into a bplus_tree defined using this bplus_tree_tuple_defs struct
	uint32_t max_index_record_size;
};

// initializes the attributes in bplus_tree_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it allocates memory only for key_element_ids, record_def, index_def and key_def
// returns 1 for success, it fails with 0, if the record_def has element_count 0 OR key_element_count == 0 OR key_element_ids == NULL OR if any of the key_element_ids is out of bounds
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, const page_access_specs* pas_p, const tuple_def* record_def, const uint32_t* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count);

// checks to see if a record_tuple can be inserted into a bplus_tree
// note :: you can not insert a NULL record in bplus_tree
int check_if_record_can_be_inserted_for_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple);

// copy all the key elements from the record_tuple to make the key
int extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, void* key);

// copy all the key elements from the index_entry to make the key
int extract_key_from_index_entry_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* index_entry, void* key);

// builds an index_entry from record_tuple and a child_page_id
int build_index_entry_from_record_tuple_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, uint64_t child_page_id, void* index_entry);

// builds an index_entry from key and a child_page_id
int build_index_entry_from_key_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* key, uint64_t child_page_id, void* index_entry);

// it deallocates the key_element_ids, record_def, index_def and key_def
// then resets all the bplus_tree_tuple_defs struct attributes to NULL or 0
void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p);

// print bplus_tree_tuple_definitions
void print_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p);

#endif