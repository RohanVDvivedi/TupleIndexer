#ifndef SORTED_PACKED_PAGE_UTIL_H
#define SORTED_PACKED_PAGE_UTIL_H

// a sorted packed page
/*
*	1. will not have any tomb stones
*	2. tuples are sorted in the increasing order given by their elements at element ids of "tuple_keys_to_compare" of the tuple in that order
*	3. page may or may not be fragmented
*/

#include<tuple_def.h>

// returns index at which a new tuple may be inserted
// returns an index in range from (0 to tuple_count) where the tuple can be inserted, keeping the sorted page sorted
// if a tuple/tuples with the given key as in "tuple" parameter exists
// than the index returns is right after these tuples that compare equals to the "tuple" parameter
uint32_t find_insertion_point_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple
								);

// insert tuple (the tuple will be inserted at the index given by find_insertion_point_in_sorted_packed_page() function)
// returns 1 tuple was inserted
// returns the index the new tuple is inserted on
int insert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t* index
								);

// insert tuple at given index (pushing elements at index >= index, down)
// returns 1, if tuple was inserted
int insert_at_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index
								);

// update the tuple at given index
// returns 1, if tuple was updated
int update_resiliently_at_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index
								);

// delete tuple at given index
// returns 1, if the tuple was deleted
int delete_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, 
									uint32_t index
								);

// insert n tuples from src page to dest page
// returns the number of tuples inserted
uint32_t insert_all_from_sorted_packed_page(
									void* page_dest, const void* page_src, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									uint32_t start_index, uint32_t end_index
								);

// delete all tuples between index range [start_index, end_index] included
// returns 1, if the tuple was deleted
int delete_all_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, 
									uint32_t start_index, uint32_t end_index
								);

#define NO_TUPLE_FOUND (~((uint32_t)0))

// returns index of the tuple found
uint32_t find_first_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found
uint32_t find_last_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the greatest index that is lesser than the key
uint32_t find_preceding_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the greatest index that is lesser than or equal to the key
// if there are tuples that compare equal to the key, then the last index of the tuple that compare equal to the key is returned
uint32_t find_preceding_equals_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the least index that is greater than or equal to the key
// if there are tuples that compare equal to the key, then the first index of the tuple that compare equal to the key is returned
uint32_t find_succeeding_equals_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the least index that is greater than the key
uint32_t find_succeeding_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// reverses the sort order on the sorted packed page
void reverse_sort_order_on_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def
								);

// creates a page into its sorted_packed_page form
void sort_and_convert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count
								);

#endif