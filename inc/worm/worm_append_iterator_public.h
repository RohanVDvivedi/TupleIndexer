#ifndef WORM_APPEND_ITERATOR_PUBLIC_H
#define WORM_APPEND_ITERATOR_PUBLIC_H

/*
	This iterator can only be used for appending to the worm
*/

typedef struct worm_append_iterator worm_append_iterator;

worm_append_iterator* get_new_worm_append_iterator(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// if data_size == 0, return 0 directly
// if data_size > 0, it returns bytes_appended for success, where 0 < bytes_appended <= data_size, bytes_appended could be non-zero lesser than data_size
// due to the above condition, you may need to call this function more than once for the same input buffer, this will not happen for sure, but the api design allows this to happen
// on an abort_error, all the pages will be unlocked by the worm_append_iterator, and return value will be 0
uint32_t append_to_worm(worm_append_iterator* wai_p, const char* data, uint32_t data_size, const void* transaction_id, int* abort_error);

void delete_worm_append_iterator(worm_append_iterator* wai_p, const void* transaction_id, int* abort_error);

#endif