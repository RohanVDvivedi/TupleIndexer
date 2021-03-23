#ifndef BPLUS_TREE_INTERIOR_PAGE_UTIL_H
#define BPLUS_TREE_INTERIOR_PAGE_UTIL_H

#include<bplus_tree_util.h>

int is_interior_page(const void* page);

/*
**
**	each tuple in the interior page is of the form 
**		: { Key : page_id(u4) }
**			where all the records greater than equal to Key and lesser than next Key are stored at the corresponding page_id
**
**	all the records lesser than the first (0th) key are stored at the page_references[ALL_LEAST_REF]
**
*/

// returns number of records in the leaf page
uint16_t get_index_entry_count_in_interior_page(const void* page);

// returns a uint32_t page_id at index-th position
// index varies from -1 to get_index_entry_count_in_interior_page() - 1
uint32_t get_index_page_id_from_interior_page(const void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds);

// returns a pointer to the tuple at index-th position that has tuple definition of index_def => {key_def, page_id}
// index varies from 0 to get_index_entry_count_in_interior_page() - 1
// the end of this index-th tuple is acccompanied with the index-th reference page_id
const void* get_index_entry_from_interior_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds);

// returns page_id, to search for the least of the tuples that has key equivalent to like_key
uint32_t find_in_interior_page(const void* page, const void* like_key, const bplus_tree_tuple_defs* bpttds);

// inserts a new_index_tuple to the intr_page
// if the insert fails due to the page being out of space
// then this interior page is split and the non-NULL index tuple is created
// now this non Null index tuple needs to be inserted into the parent interior page
// the retunred tuple is allocated using malloc and the calling function has to deallocate it after use/insert
const void* insert_or_split_interior_page(const void* intr_page, const void* new_index_tuple, const bplus_tree_tuple_defs* bpttds);

// merge 2 pages that are child pages to intr_page each of then is an adjacent page_id to the tuple at given index
const void* merge_child_pages_at(const void* intr_page, uint16_t index, const bplus_tree_tuple_defs* bpttds);

#endif