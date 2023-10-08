#ifndef BPLUS_TREE_TUPLE_DEFINITIONS_H
#define BPLUS_TREE_TUPLE_DEFINITIONS_H

#include<tuple.h>
#include<inttypes.h>

typedef struct bplus_tree_tuple_defs bplus_tree_tuple_defs;
struct bplus_tree_tuple_defs
{
	// page_id_witdh is bytes required for storing page_id, it can be in range [1, 8] both inclusive
	uint8_t page_id_width;

	// size of each page inside this bplus tree
	uint32_t page_size;

	// this is what is considered as a NULL pointer in the bplus_tree
	// NULL_PAGE_ID < (1 << (page_id_width * 8))
	uint64_t NULL_PAGE_ID;

	// this is the additional page header space left out by the library for your use
	// any page that will be used by the library for the bplus_tree will have page_header size of this plus the ones additionally required by the specific page type
	// this many number of bytes will be left in the preface of the page_header and will be left untouched
	uint32_t system_header_size;

	// number of elements considered as keys
	uint32_t key_element_count;

	// element ids of the keys (as per their element_ids in record_def) in the order as you want them to be ordered
	uint32_t* key_element_ids;

	// compare direction for the keys, array of ASC/DESC
	const compare_direction* key_compare_direction;

	// ith element_def in index_def has the same type, name and size as the key_element_ids[i] th element_def in record_def

	// tuple definition of the leaf pages in the bplus_tree
	tuple_def* record_def;

	// tuple definition of the interior pages in the bplus_tree
	tuple_def* index_def;

	// tuple definition of the key to be used with this bplus_tree
	// for all of find, insert and delete functionalities
	tuple_def* key_def;

	// precomputed value of max_record_size (for the tuple that goes into leaf pages of the bplus_tree) that can be inserted into a bplus_tree defined using this bplus_tree_tuple_defs struct
	uint32_t max_record_size;

	// precomputed value of max_index_record_size (for the tuple that goes into interior pages of the bplus_tree) that can be inserted into a bplus_tree defined using this bplus_tree_tuple_defs struct
	uint32_t max_index_record_size;
};

// initializes the attributes in bplus_tree_tuple_defs struct as per the provided parameters
// it allocates memory only for index_def and the key_def
// returns 1 for success, it fails with 0, if the record_def has element_count 0 OR key_element_count == 0 OR key_elements == NULL OR if any of the key_element_ids is out of bounds
// it may also fail if the system_header size makes it impossible to store any tuples in the page
// page_id_width is bytes required for storing page_id, it can be 1,2,4 or 8
int init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, uint32_t system_header_size, const tuple_def* record_def, const uint32_t* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, uint32_t page_size, uint8_t page_id_width, uint64_t NULL_PAGE_ID);

// checks to see if a record_tuple can be inserted into a bplus_tree
int check_if_record_can_be_inserted_into_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple);

// copy all the key elements from the record_tuple to make the key
void extract_key_from_record_tuple(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, void* key);

// copy all the key elements from the index_entry to make the key
void extract_key_from_index_entry(const bplus_tree_tuple_defs* bpttd_p, const void* index_entry, void* key);

// builds an index_entry from record_tuple and a child_page_id
void build_index_entry_from_record_tuple(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, uint64_t child_page_id, void* index_entry);

// builds an index_entry from key and a child_page_id
void build_index_entry_from_key(const bplus_tree_tuple_defs* bpttd_p, const void* key, uint64_t child_page_id, void* index_entry);

// it deallocates the index_def and
// then resets all the bplus_tree_tuple_defs struct attributes to NULL or 0
void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p);

// print bplus_tree_tuple_definitions
void print_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p);

#endif