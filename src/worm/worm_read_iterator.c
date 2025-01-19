#include<worm_read_iterator.h>

#include<persistent_page_functions.h>
#include<worm_page_header.h>

worm_read_iterator* get_new_worm_read_iterator(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

worm_read_iterator* clone_worm_read_iterator(worm_read_iterator* wri, const void* transaction_id, int* abort_error)
{
	// TODO
}

uint32_t read_from_worm(worm_read_iterator* wri, char* data, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	// TODO
}

worm_read_iterator* delete_worm_read_iterator(worm_read_iterator* wri, const void* transaction_id, int* abort_error)
{
	// TODO
}