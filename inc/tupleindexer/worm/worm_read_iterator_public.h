#ifndef WORM_READ_ITERATOR_PUBLIC_H
#define WORM_READ_ITERATOR_PUBLIC_H

/*
	This iterator can only be used for reading from the worm (in forward direction only)
*/

/*
	It will only keep the current page that we are looking-at read-locked, and always latch crab forward
*/

typedef struct worm_read_iterator worm_read_iterator;

worm_read_iterator* get_new_worm_read_iterator(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

worm_read_iterator* clone_worm_read_iterator(const worm_read_iterator* wri_p, const void* transaction_id, int* abort_error);

// since worm_read_iterator only keeps the curr_page that it is looking at read locked
// you can only use this function if the worm_read_iterator is at the head page (not necessarily at it's first byte)
// so if you intend to use this function, then use it directly after opening the iterator, as there is not mechanism provided to know if the worm_read_iterator is at the head page
// this means this functions MAY not work for cloned worm_read_iterators
// With an indeterminate position when not at head, this function fails silently with returning NULL_PAGE_ID
uint64_t get_dependent_root_page_id_worm_read_iterator(const worm_read_iterator* wri_p);

// if data_size == 0, then 0 is returned right away
// if data == NULL, then a forward relative seek is performed for data_size bytes
// if data_size > 0, then a 0 is returned only if there are no more bytes in worm to be read
// lesser than data_size bytes are read, possibly because of you reaching the end of the worm
// on an abort_error all locks are released and 0 is returned
uint32_t read_from_worm(worm_read_iterator* wri_p, char* data, uint32_t data_size, const void* transaction_id, int* abort_error);

// this function can be used to directly peek the next available consecutive bytes from worm
// this function allows peeking directly into the page, by returning the memory pointer on the page, pointing to data_size number of bytes
// returns NULL and 0, if there are no more bytes/blobs in the worm, this implies end of worm
// peek will not move the iterator forward, you will need to call read_from_worm(wri_p, NULL, data_size, transaction_id, &abort_error); to make it move forward by that many bytes
// on an abort_error all locks are released and NULL and 0 is returned
const char* peek_in_worm(worm_read_iterator* wri_p, uint32_t* data_size, const void* transaction_id, int* abort_error);

void delete_worm_read_iterator(worm_read_iterator* wri_p, const void* transaction_id, int* abort_error);

#endif