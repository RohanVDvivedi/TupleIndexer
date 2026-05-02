#include<tupleindexer/blob_store/blob_store_read_iterator.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<stdlib.h>

blob_store_read_iterator* get_new_blob_store_read_iterator(uint64_t head_page_id, uint32_t curr_tuple_index, uint32_t curr_byte_index, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

blob_store_read_iterator* clone_blob_store_read_iterator(const blob_store_read_iterator* bsri_p, const void* transaction_id, int* abort_error)
{
	// allocate enough memory
	blob_store_read_iterator* clone_p = malloc(sizeof(blob_store_read_iterator));
	if(clone_p == NULL)
		exit(-1);

	clone_p->curr_page = acquire_persistent_page_with_lock(bsri_p->pam_p, transaction_id, bsri_p->curr_page.page_id, READ_LOCK, abort_error);
	if(*abort_error)
	{
		free(clone_p);
		return NULL;
	}
	clone_p->curr_tuple_index = bsri_p->curr_tuple_index;
	clone_p->curr_byte_index = bsri_p->curr_byte_index;
	clone_p->bstd_p = bsri_p->bstd_p;
	clone_p->pam_p = bsri_p->pam_p;

	return clone_p;
}

uint32_t read_from_blob(blob_store_read_iterator* bsri_p, char* data, uint32_t data_size, const void* transaction_id, int* abort_error);

const char* peek_in_blob(blob_store_read_iterator* bsri_p, uint32_t* data_size, const void* transaction_id, int* abort_error);

uint64_t get_position_in_blob(const blob_store_read_iterator* bsri_p, uint32_t* curr_tuple_index, uint32_t* curr_byte_index)
{
	(*curr_byte_index) = bsri_p->curr_byte_index;
	(*curr_tuple_index) = bsri_p->curr_tuple_index;
	return bsri_p->curr_page.page_id;
}

void delete_blob_store_read_iterator(blob_store_read_iterator* bsri_p, const void* transaction_id, int* abort_error)
{
	// if curr_page is still locked, then release this lock
	// it may not be locked, if we encountered an abort error
	if(!is_persistent_page_NULL(&(bsri_p->curr_page), bsri_p->pam_p))
		release_lock_on_persistent_page(bsri_p->pam_p, transaction_id, &(bsri_p->curr_page), NONE_OPTION, abort_error);

	free(bsri_p);
}