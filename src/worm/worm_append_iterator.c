#include<worm_append_iterator.h>

#include<worm_page_util.h>

#include<worm_head_page_header.h>
#include<worm_any_page_header.h>
#include<worm_page_header.h>

worm_append_iterator* get_new_worm_append_iterator(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 3 must be present
	if(wtd_p == NULL || pam_p == NULL || pmm_p == NULL)
		return NULL;

	// allocate enough memory
	worm_append_iterator* wai_p = malloc(sizeof(worm_append_iterator));
	if(wai_p == NULL)
		exit(-1);

	wai_p->head_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
	{
		free(wai_p);
		return NULL;
	}
	wai_p->wtd_p = wtd_p;
	wai_p->pam_p = pam_p;
	wai_p->pmm_p = pmm_p;

	return wai_p;
}

int append_to_worm(worm_append_iterator* wai_p, const char* data, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	// no data to append is always a success
	if(data_size == 0)
		return 1;

	// TODO
}

int delete_worm_append_iterator(worm_append_iterator* wai_p, const void* transaction_id, int* abort_error)
{
	// TODO
}