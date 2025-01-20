#include<worm_read_iterator.h>

#include<persistent_page_functions.h>
#include<worm_page_header.h>

worm_read_iterator* get_new_worm_read_iterator(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(wtd_p == NULL || pam_p == NULL)
		return NULL;

	// allocate enough memory
	worm_read_iterator* wri_p = malloc(sizeof(worm_read_iterator));
	if(wri_p == NULL)
		exit(-1);

	wri_p->curr_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, READ_LOCK, abort_error);
	if(*abort_error)
	{
		free(wri_p);
		return NULL;
	}
	wri_p->curr_blob_index = 0;
	wri_p->curr_byte_index = 0; // this position is always present OR the worm is empty and you are at it's end
	wri_p->wtd_p = wtd_p;
	wri_p->pam_p = pam_p;

	return wri_p;
}

worm_read_iterator* clone_worm_read_iterator(worm_read_iterator* wri_p, const void* transaction_id, int* abort_error)
{
	// allocate enough memory
	worm_read_iterator* clone_p = malloc(sizeof(worm_read_iterator));
	if(clone_p == NULL)
		exit(-1);

	clone_p->curr_page = acquire_persistent_page_with_lock(wri_p->pam_p, transaction_id, wri_p->curr_page.page_id, READ_LOCK, abort_error);
	if(*abort_error)
	{
		free(clone_p);
		return NULL;
	}
	clone_p->curr_blob_index = wri_p->curr_blob_index;
	clone_p->curr_byte_index = wri_p->curr_byte_index;
	clone_p->wtd_p = wri_p->wtd_p;
	clone_p->pam_p = wri_p->pam_p;

	return clone_p;
}

uint32_t read_from_worm(worm_read_iterator* wri_p, char* data, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	// TODO
}

void delete_worm_read_iterator(worm_read_iterator* wri_p, const void* transaction_id, int* abort_error)
{
	// if curr_page is still locked, then release this lock
	// it may not be locked, if we encountered an abort error
	if(!is_persistent_page_NULL(&(wri_p->curr_page), wri_p->pam_p))
		release_lock_on_persistent_page(wri_p->pam_p, transaction_id, &(wri_p->curr_page), NONE_OPTION, abort_error);
	free(wri_p);
}