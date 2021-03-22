#ifndef SORTED_PACKED_PAGE_UTIL_H
#define SORTED_PACKED_PAGE_UTIL_H

// find using binary search in sorted page
// returns index of the tuple found if, return value == 1
// else returns index to the element just lesser than the the key with return value 0
int search_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* key_val_def, 
									const void* key, 
									uint16_t* index
								);

// insert tuple
// returns the index of the inserted tuple if, return value == 1
int insert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* key_val_def, 
									const void* tuple, 
									uint16_t* index
								);

// delete tuple at given index
// returns 1, if the tuple was deleted
int delete_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* key_val_def, 
									uint16_t index
								);

// move n tuples from src page to dest page
// effectively the tuples moved, will be deleted in src_page and appear in dest_page
int transfer_sorted_packed_page(
									void* src_page, void* dest_page, uint32_t page_size, 
									const tuple_def* key_val_def, 
									uint16_t start_index, uint16_t end_index
								);

#endif