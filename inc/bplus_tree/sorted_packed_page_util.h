#ifndef SORTED_PACKED_PAGE_UTIL_H
#define SORTED_PACKED_PAGE_UTIL_H

// a sorted packed page
/*
*	1. will not have any tomb stones
*	2. tuples are sorted in the increasing order given by their elements at element ids of "tuple_keys_to_compare" of the tuple in that order
*	3. page may or may not be fragmented
*/

#include<tuple.h>
#include<persistent_page.h>
#include<opaque_page_modification_methods.h>

/*
** Generic definitions of the parameter to be passed to the functions in sorted packed page
**	persistent_page       ppage                           -> the actual persistent page to work on
**	uint32_t              page_size                       -> the size of the page in bytes
**	tuple_def*            tpl_def                         -> the definition of each tuple on the ppage
**	uint32_t*             tuple_keys_to_compare           -> the indices of the element_defs in the tpl_def, that ppage is sorted on, (for find operations you may provide lesser number of keys)
**	compare_direction*    tuple_keys_compare_direction    -> the sort direction of each of the elements as in tuple_keys_to_compare, in the order of comparison
**	uint32_t              keys_count                      -> the number of elements in tuple_keys_to_compare and tuple_keys_to_compare_direction
**
** above attributes define what is there on the page
**
**  void*                 tuple                           -> tuple to be inserted/updated
**	uint32_t              index                           -> the index to the tuple on the page to work with
**
** below are the attributes required by the find functions on the sorted_packed_page
**
**	void*                 key                             -> we need to find a tuple on the ppage, with respect to this key
**	tuple_def*            key_def                         -> tuple definition of the key, provided
**	uint32_t*             key_elements_to_compare         -> the element ids on the key, that correspond to the tuple_keys_to_compare, in the order of comparison, its length will be same as keys_count parameter
**
** NOTE: length of the arrays tuple_keys_to_compare, tuple_keys_compare_direction and key_elements_to_compare, must be atleast keys_count
** NOTE: if the sorted_packed ppage is lets say sorted on tuple_keys_to_compare = {7 , 3, 1} element ids,
**			then ONLY find functions are allowed to use shorter tuple_keys_to_compare,
**				i.e. for ONLY the find function you may either pass {7} or {7,3} or {7,3,1} as tuple_keys_to_compare
**
*/

// returns index at which a new tuple may be inserted
// returns an index in range from (0 to tuple_count) where the tuple can be inserted, keeping the sorted page sorted
// if a tuple/tuples with the given key as in "tuple" parameter exists
// than the index returns is right after these tuples that compare equals to the "tuple" parameter
uint32_t find_insertion_point_in_sorted_packed_page(
									const persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* tuple
								);

// insert tuple (the tuple will be inserted at the index given by find_insertion_point_in_sorted_packed_page() function)
// returns 1 tuple was inserted
// returns the index the new tuple is inserted on
int insert_to_sorted_packed_page(
									persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* tuple, 
									uint32_t* index,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

// insert tuple at given index (pushing elements at index >= index, down)
// returns 1, if tuple was inserted
int insert_at_in_sorted_packed_page(
									persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* tuple, 
									uint32_t index,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

// update the tuple at given index
// returns 1, if tuple was updated
int update_at_in_sorted_packed_page(
									persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* tuple, 
									uint32_t index,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

// delete tuple at given index
// returns 1, if the tuple was deleted
int delete_in_sorted_packed_page(
									persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, 
									uint32_t index,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

// insert n number of tuples [start_index, last_index] (both inclusive) from src page to dest page
// returns the number of tuples inserted
uint32_t insert_all_from_sorted_packed_page(
									persistent_page* ppage_dest, const persistent_page* ppage_src, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									uint32_t start_index, uint32_t last_index,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

// delete all tuples between index range [start_index, last_index] included
// returns 1, if the tuple was deleted
int delete_all_in_sorted_packed_page(
									persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, 
									uint32_t start_index, uint32_t last_index,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

#define NO_TUPLE_FOUND INVALID_TUPLE_INDEX

// returns index of the tuple found
uint32_t find_first_in_sorted_packed_page(
									const persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found
uint32_t find_last_in_sorted_packed_page(
									const persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the greatest index that is lesser than the key
uint32_t find_preceding_in_sorted_packed_page(
									const persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the greatest index that is lesser than or equal to the key
// if there are tuples that compare equal to the key, then the last index of the tuple that compare equal to the key is returned
uint32_t find_preceding_equals_in_sorted_packed_page(
									const persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the least index that is greater than or equal to the key
// if there are tuples that compare equal to the key, then the first index of the tuple that compare equal to the key is returned
uint32_t find_succeeding_equals_in_sorted_packed_page(
									const persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// returns index of the tuple found at the least index that is greater than the key
uint32_t find_succeeding_in_sorted_packed_page(
									const persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								);

// reverses the sort order on the sorted packed page
void reverse_sort_order_on_sorted_packed_page(
									persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

// creates a page into its sorted_packed_page form
void sort_and_convert_to_sorted_packed_page(
									persistent_page* ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, const compare_direction* tuple_keys_compare_direction, uint32_t keys_count,
									const page_modification_methods* pmm_p,
									const void* transaction_id,
									int* abort_error
								);

#endif