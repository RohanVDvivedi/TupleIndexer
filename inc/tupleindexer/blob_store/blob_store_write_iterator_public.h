#ifndef BLOB_STORE_WRITE_ITERATOR_PUBLIC_H
#define BLOB_STORE_WRITE_ITERATOR_PUBLIC_H

/*
	you must have a write lock or exclusive lock on the resource to use these functions
*/

/*
	This iterator can only be used for append_to_tail/discard_from_head in the blob_store
*/

typedef struct blob_store_write_iterator blob_store_write_iterator;

/*
	only heed head to discard to head and tail to append to tail

	if both head and tail are NULL_PAGE_ID, a new list of blob bigibs with head and tail both populated

	it holds only minimal locks for both the operation and does not do latch crabbing as we are now operating with stable pointers
*/

blob_store_write_iterator* get_new_blob_store_write_iterator(uint64_t root_page_id, uint64_t head_page_id, uint32_t head_tuple_index, uint64_t tail_page_id, uint32_t tail_tuple_index, const blob_store_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns true only if both head and tail have NULL_PAGE_ID
int is_empty_blob_in_blob_store(const blob_store_write_iterator* bswi_p);

// returns head_page_id
uint64_t get_head_position_in_blob(const blob_store_write_iterator* bswi_p, uint32_t* head_tuple_index);

// tail_byte_index will be the size of the binary in the tail
uint64_t get_tail_position_in_blob(const blob_store_write_iterator* bswi_p, uint32_t* tail_tuple_index, uint32_t* tail_byte_index);

#include<tupleindexer/heap_table/heap_table.h> // only to make the heap_table_notifier available here

// if data_size == 0, return 0 directly
// if data_size > 0, it returns bytes_appended for success, where 0 < bytes_appended <= data_size, bytes_appended could be non-zero lesser than data_size
// on an abort_error, all the pages will be unlocked by the blob_store_write_iterator, and return value will be 0
uint32_t append_to_tail_in_blob(blob_store_write_iterator* bswi_p, const heap_table_notifier* notify_wrong_entry, const char* data, uint32_t data_size, const void* transaction_id, int* abort_error);

// if data_size == 0, return 0 directly
// if data_size > 0, it returns bytes_discarded for success, where 0 < bytes_discarded <= data_size, bytes_discarded could be non-zero lesser than data_size
// on an abort_error, all the pages will be unlocked by the blob_store_write_iterator, and return value will be 0
uint32_t discard_from_head_in_blob(blob_store_write_iterator* bswi_p, const heap_table_notifier* notify_wrong_entry, uint32_t data_size, const void* transaction_id, int* abort_error);

// both the above functions try to modify minimal pages while appending a singular chunk of data
// this allows you to brek your insets into smaller and smaller transactions

void delete_blob_store_write_iterator(blob_store_write_iterator* bswi_p, const void* transaction_id, int* abort_error);

#endif