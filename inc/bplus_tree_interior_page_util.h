#ifndef BPLUS_TREE_INTERIOR_PAGE_UTIL_H
#define BPLUS_TREE_INTERIOR_PAGE_UTIL_H

#include<bplus_tree_util.h>

/*
**
**	each tuple in the interior page is of the form 
**		: { Key : page_id(u4) }
**			where all the records greater than equal to Key and lesser than next Key are stored at the corresponding page_id
**
**	all the records lesser than the first (0th) key are stored at the page_references[ALL_LEAST_REF]
**
*/

int is_interior_page(const void* page);

// to initialize an interior page
int init_interior_page(void* page, uint32_t page_size, const bplus_tree_tuple_defs* bpttds);

// returns number of records in the leaf page
uint16_t get_index_entry_count_in_interior_page(const void* page);

// returns a uint32_t page_id at index-th position
// index varies from -1 to get_index_entry_count_in_interior_page() - 1
uint32_t get_index_page_id_from_interior_page(const void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds);

// returns 1, if a uint32_t page_id at index-th position is set to page_id
// index varies from -1 to get_index_entry_count_in_interior_page() - 1
int set_index_page_id_in_interior_page(void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds, uint32_t page_id);

// returns a pointer to the tuple at index-th position that has tuple definition of index_def => {key_def, page_id}
// index varies from 0 to get_index_entry_count_in_interior_page() - 1
// the end of this index-th tuple is acccompanied with the index-th reference page_id
const void* get_index_entry_from_interior_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds);

// returns index to the index_page_id, to search for the tuple with like_key
int32_t find_in_interior_page(const void* page, uint32_t page_size, const void* like_key, const bplus_tree_tuple_defs* bpttds);

// below functions manage splits and merges of the leaf_pages of the b+tree

// returns the tuple (of bpttds->tuple_def) that we need to insert into the parent page
// returns non NULL tuple if the page was split
void* split_insert_interior_page(void* page_to_be_split, const void* new_index_entry, void* new_page, uint32_t new_page_id, uint32_t page_size, const bplus_tree_tuple_defs* bpttds);

// returns true (1), if parent_index_record needs to be deleted from the parent_page
int merge_interior_pages(void* page, const void* parent_index_record, void* sibling_page_to_be_merged, uint32_t page_size, const bplus_tree_tuple_defs* bpttds);

#endif