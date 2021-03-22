#include<sorted_packed_page_util.h>

int search_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* tuple_def, 
									const void* key, 
									uint16_t* index
								);

int insert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* tuple_def, 
									const void* tuple, 
									uint16_t* index
								);

int delete_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tuple_def, 
									uint16_t index
								);

int transfer_sorted_packed_page(
									void* src_page, void* dest_page, uint32_t page_size, 
									const tuple_def* tuple_def, 
									uint16_t start_index, uint16_t end_index
								);