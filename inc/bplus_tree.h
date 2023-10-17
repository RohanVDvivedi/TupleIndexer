#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include<bplus_tree_iterator.h>

#include<bplus_tree_tuple_definitions.h>
#include<opaque_data_access_methods.h>
#include<opaque_page_modification_methods.h>

// returns pointer to the root page of the bplus_tree
uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p);

// order of the enum values in find_position must remain the same
typedef enum find_position find_position;
enum find_position
{
	LESSER_THAN,
	LESSER_THAN_EQUALS,
	GREATER_THAN_EQUALS,
	GREATER_THAN,
};

// this macro can be passed to key_element_count_concerned (to find_in_bplus_tree), to consider all the key_elements as found in bpttd_p(->key_element_count)
#define KEY_ELEMENT_COUNT UINT32_C(-1)

// returns a bplus_tree_iterator to read from key 
// here the key_element_count_concerned suggests the number of elements of the key that would be considered for find operation
// the key == NULL and find_pos == GREATER_THAN, then the iterator will point to the first tuple of the bplus_tree
// the key == NULL and find_pos == LESSER_THAN, then the iterator will point to the last tuple of the bplus_tree
bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

typedef struct update_inspector update_inspector;
struct update_inspector
{
	const void* context;

	// the operation for an update will be performed only if the update_inspector returns 1, 0 implies no operation
	// new_record can be modified by this function, (but mind well to keep all the key elements of the record un touched)
	// upon the return of 1 from this function
	// if(old_record == NULL && *new_record == NULL) -> do nothing
	// else if(old_record != NULL && *new_record == NULL) -> do delete of the old_record
	// else if(old_record == NULL && *new_record != NULL) -> do insert for the new_record
	// else if(old_record != NULL && *new_record != NULL) -> do update old_record with the new_record
	int update_inspect(const void* context, const tuple_def* record_def, const void* old_record, void** new_record);
};

// to find and read a record, then inspect it with the ui_p, and then proceed to update it
int inspected_update_in_bplus_tree(uint64_t root_page_id, const void* new_record, const update_inspector* ui_p, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p);

// insert record in bplus_tree
int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p);

// delete a record given by key
int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p);

// frees all the pages occupied by the bplus_tree
int destroy_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// prints all the pages in the bplus_tree
void print_bplus_tree(uint64_t root_page_id, int only_leaf_pages, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

#endif