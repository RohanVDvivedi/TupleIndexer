#ifndef BLOB_STORE_READ_ITERATOR_PUBLIC_H
#define BLOB_STORE_READ_ITERATOR_PUBLIC_H

/*
	This iterator can only be used for reading a particular blob from the blob_store (in forward direction only)
*/

/*
	It will only keep the current page that we are looking-at read-locked at any instance, and never latch crab forward, it will instead release curr latch and then latch the next page
*/

typedef struct blob_store_read_iterator blob_store_read_iterator;

blob_store_read_iterator* get_new_blob_store_read_iterator(uint64_t head_page_id, uint32_t curr_tuple_index, uint32_t curr_byte_index, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

blob_store_read_iterator* clone_blob_store_read_iterator(const blob_store_read_iterator* wri_p, const void* transaction_id, int* abort_error);

// if data_size == 0, then 0 is returned right away
// if data == NULL, then a forward relative seek is performed for data_size bytes
// if data_size > 0, then a 0 is returned only if there are no more bytes in blob_store to be read
// lesser than data_size bytes are read, possibly because of you have reached the end of the blob_store
// on an abort_error all locks are released and 0 is returned
uint32_t read_from_blob_store(blob_store_read_iterator* bsri_p, char* data, uint32_t data_size, const void* transaction_id, int* abort_error);

// this function can be used to directly peek the next available consecutive bytes from blob_store
// this function allows peeking directly into the page, by returning the memory pointer on the page, pointing to data_size number of bytes
// returns NULL and 0, if there are no more bytes/binarys in the blob_store, this implies end of blob_store
// peek will not move the iterator forward, you will need to call read_from_blob_store(wri_p, NULL, data_size, transaction_id, &abort_error); to make it move forward by that many bytes
// on an abort_error all locks are released and NULL and 0 is returned
const char* peek_in_blob_store(blob_store_read_iterator* bsri_p, uint32_t* data_size, const void* transaction_id, int* abort_error);

// get current position of the blob_store_read_iterator, the curr_binary_index and curr_byte_index are output parameters, and the return value is the curr_page_id of the blob_store page that we are in
uint64_t get_position_in_blob_store(blob_store_read_iterator* bsri_p, uint32_t* curr_tuple_index, uint32_t* curr_byte_index);

void delete_blob_store_read_iterator(blob_store_read_iterator* bsri_p, const void* transaction_id, int* abort_error);

#endif