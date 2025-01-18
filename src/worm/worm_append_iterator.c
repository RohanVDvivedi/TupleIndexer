#include<worm_append_iterator.h>

#include<worm_page_util.h>

#include<worm_head_page_header.h>
#include<worm_any_page_header.h>
#include<worm_page_header.h>

worm_append_iterator* get_new_worm_append_iterator(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int append_to_worm(worm_append_iterator* wai_p, const char* data, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	// TODO
}

int delete_worm_append_iterator(worm_append_iterator* wai_p, const void* transaction_id, int* abort_error)
{
	// TODO
}