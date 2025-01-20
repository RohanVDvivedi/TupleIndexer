#ifndef WORM_READ_ITERATOR_PUBLIC_H
#define WORM_READ_ITERATOR_PUBLIC_H

/*
	This iterator can only be used for reading from the worm (in forward direction only)
*/

/*
	IT will only keep the current page that we re looking at locked, and always latch crab forward
*/

typedef struct worm_read_iterator worm_read_iterator;

worm_read_iterator* get_new_worm_read_iterator(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

worm_read_iterator* clone_worm_read_iterator(worm_read_iterator* wri_p, const void* transaction_id, int* abort_error);

// if data_size == 0, then 0 is returned right away
// if data == NULL, then a forward relative seek is performed for data_size bytes
// if data_size > 0, then a 0 is returned only if there are no more bytes in worm to be read
// lesser than data_size bytes are read, possibly because of you reaching the endof the worm
// on an abort_error all locks are released and 0 is returned
uint32_t read_from_worm(worm_read_iterator* wri_p, char* data, uint32_t data_size, const void* transaction_id, int* abort_error);

void delete_worm_read_iterator(worm_read_iterator* wri_p, const void* transaction_id, int* abort_error);

#endif