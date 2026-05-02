#include<tupleindexer/blob_store/blob_store_read_iterator.h>

#include<tupleindexer/blob_store/blob_store_chunk_util.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<stdlib.h>

static void refresh_unread_chunk_data_for_blob_store_read_iterator(blob_store_read_iterator* bsri_p)
{
	bsri_p->unread_chunk_data = (*NULL_DATUM);

	if(is_persistent_page_NULL(&(bsri_p->curr_page), bsri_p->pam_p))
		return;

	const void* chunk = get_nth_tuple_on_persistent_page(&(bsri_p->curr_page), bsri_p->bstd_p->pas_p->page_size, &(bsri_p->bstd_p->chunk_tuple_def->size_def), bsri_p->curr_tuple_index);
	if(chunk == NULL)
		return ;

	bsri_p->unread_chunk_data = get_curr_chunk_data(chunk, bsri_p->bstd_p);
	if(is_datum_NULL(&(bsri_p->unread_chunk_data)))
		return ;

	bsri_p->unread_chunk_data.binary_value += bsri_p->curr_byte_index;
	bsri_p->unread_chunk_data.binary_size -= bsri_p->curr_byte_index;
}

blob_store_read_iterator* get_new_blob_store_read_iterator(uint64_t head_page_id, uint32_t curr_tuple_index, uint32_t curr_byte_index, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(bstd_p == NULL || pam_p == NULL)
		return NULL;

	// allocate enough memory
	blob_store_read_iterator* bsri_p = malloc(sizeof(blob_store_read_iterator));
	if(bsri_p == NULL)
		exit(-1);

	if(head_page_id == bsri_p->bstd_p->pas_p->NULL_PAGE_ID)
		bsri_p->curr_page = get_NULL_persistent_page(bsri_p->pam_p);
	else
	{
		bsri_p->curr_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, READ_LOCK, abort_error);
		if(*abort_error)
		{
			free(bsri_p);
			return NULL;
		}
	}
	bsri_p->curr_tuple_index = curr_tuple_index;
	bsri_p->curr_byte_index = curr_byte_index;
	refresh_unread_chunk_data_for_blob_store_read_iterator(bsri_p);
	bsri_p->bstd_p = bstd_p;
	bsri_p->pam_p = pam_p;

	return bsri_p;
}

blob_store_read_iterator* clone_blob_store_read_iterator(const blob_store_read_iterator* bsri_p, const void* transaction_id, int* abort_error)
{
	// allocate enough memory
	blob_store_read_iterator* clone_p = malloc(sizeof(blob_store_read_iterator));
	if(clone_p == NULL)
		exit(-1);

	if(is_persistent_page_NULL(&(bsri_p->curr_page), bsri_p->pam_p))
		clone_p->curr_page = get_NULL_persistent_page(bsri_p->pam_p);
	else
	{
		clone_p->curr_page = acquire_persistent_page_with_lock(bsri_p->pam_p, transaction_id, bsri_p->curr_page.page_id, READ_LOCK, abort_error);
		if(*abort_error)
		{
			free(clone_p);
			return NULL;
		}
	}
	clone_p->curr_tuple_index = bsri_p->curr_tuple_index;
	clone_p->curr_byte_index = bsri_p->curr_byte_index;
	refresh_unread_chunk_data_for_blob_store_read_iterator(clone_p);
	clone_p->bstd_p = bsri_p->bstd_p;
	clone_p->pam_p = bsri_p->pam_p;

	return clone_p;
}

// makes the iterator point to next chunk from the current position
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_next_page(blob_store_read_iterator* bsri_p, const void* transaction_id, int* abort_error)
{
	uint64_t next_page_id = bsri_p->bstd_p->pas_p->NULL_PAGE_ID;
	uint32_t next_tuple_index = 0;

	// find the next_page_id and next_tuple_index (we already know that the next_byte_index = 0)
	{
		if(is_persistent_page_NULL(&(bsri_p->curr_page), bsri_p->pam_p))
			return 0;

		const void* chunk = get_nth_tuple_on_persistent_page(&(bsri_p->curr_page), bsri_p->bstd_p->pas_p->page_size, &(bsri_p->bstd_p->chunk_tuple_def->size_def), bsri_p->curr_tuple_index);
		if(chunk != NULL)
			next_page_id = get_next_chunk_pointer(chunk, &next_tuple_index, bsri_p->bstd_p);
	}

	// release lock on the curr_page
	release_lock_on_persistent_page(bsri_p->pam_p, transaction_id, &(bsri_p->curr_page), NONE_OPTION, abort_error);
	if(*abort_error)
		return 0;

	// lock the next_page as curr_page, and set the pointers on the page
	if(next_page_id != bsri_p->bstd_p->pas_p->NULL_PAGE_ID)
	{
		bsri_p->curr_page = acquire_persistent_page_with_lock(bsri_p->pam_p, transaction_id, next_page_id, READ_LOCK, abort_error);
		if(*abort_error)
			return 0;
	}
	bsri_p->curr_tuple_index = next_tuple_index;
	bsri_p->curr_byte_index = 0;

	// refresh the unread chunk data
	refresh_unread_chunk_data_for_blob_store_read_iterator(bsri_p);

	// goto_next was a success if next_leaf_page is not null
	return !is_persistent_page_NULL(&(bsri_p->curr_page), bsri_p->pam_p);
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