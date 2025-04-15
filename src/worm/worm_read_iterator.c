#include<tupleindexer/worm/worm_read_iterator.h>

#include<tupleindexer/utils/persistent_page_functions.h>
#include<tupleindexer/worm/worm_page_header.h>

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

uint64_t get_dependent_root_page_id_worm_read_iterator(const worm_read_iterator* wri_p)
{
	// it is not the head page fail silently
	if(!is_worm_head_page(&(wri_p->curr_page), wri_p->wtd_p))
		return wri_p->wtd_p->pas_p->NULL_PAGE_ID;

	return get_worm_head_page_header(&(wri_p->curr_page), wri_p->wtd_p).dependent_root_page_id;
}

// makes the iterator point to next page of the curr_page
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_next_page(worm_read_iterator* wri_p, const void* transaction_id, int* abort_error)
{
	// get the next_page_id
	uint64_t next_page_id = get_next_page_id_of_worm_page(&(wri_p->curr_page), wri_p->wtd_p);

	// attempt to lock the next_page
	persistent_page next_page = get_NULL_persistent_page(wri_p->pam_p);
	if(next_page_id != wri_p->wtd_p->pas_p->NULL_PAGE_ID)
	{
		next_page = acquire_persistent_page_with_lock(wri_p->pam_p, transaction_id, next_page_id, READ_LOCK, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(wri_p->pam_p, transaction_id, &(wri_p->curr_page), NONE_OPTION, abort_error);
			return 0;
		}
	}

	// release lock on the curr_page
	release_lock_on_persistent_page(wri_p->pam_p, transaction_id, &(wri_p->curr_page), NONE_OPTION, abort_error);
	if(*abort_error)
	{
		// on an abort error release lock on next_page if it is not NULL
		if(!is_persistent_page_NULL(&next_page, wri_p->pam_p))
			release_lock_on_persistent_page(wri_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
		return 0;
	}

	// update the curr_page
	wri_p->curr_page = next_page;

	// goto_next was a success if next_leaf_page is not null
	return !is_persistent_page_NULL(&(wri_p->curr_page), wri_p->pam_p);
}

// returns if the above function will succeed
static int can_goto_next_page(const worm_read_iterator* wri_p)
{
	//can go next only if the next_page_id of the curr_page is not NULL_PAGE_ID
	return (get_next_page_id_of_worm_page(&(wri_p->curr_page), wri_p->wtd_p) != wri_p->wtd_p->pas_p->NULL_PAGE_ID);
}

uint32_t read_from_worm(worm_read_iterator* wri_p, char* data, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	if(data_size == 0)
		return 0;

	uint32_t bytes_read = 0;

	while(data_size > 0)
	{
		// if you are blob_index is out of bounds, then change the page
		if(wri_p->curr_blob_index >= get_tuple_count_on_persistent_page(&(wri_p->curr_page), wri_p->wtd_p->pas_p->page_size, &(wri_p->wtd_p->partial_blob_tuple_def->size_def)))
		{
			// if there is no next page to go to, then break out of the loop
			if(!can_goto_next_page(wri_p))
				break;

			// goto the next page
			goto_next_page(wri_p, transaction_id, abort_error);
			if(*abort_error)
				return 0;

			wri_p->curr_blob_index = 0;
			wri_p->curr_byte_index = 0;
		}

		// get the curr_blob, the tuple at curr_blob_index
		user_value curr_blob;
		{
			const void* blob_tuple = get_nth_tuple_on_persistent_page(&(wri_p->curr_page), wri_p->wtd_p->pas_p->page_size, &(wri_p->wtd_p->partial_blob_tuple_def->size_def), wri_p->curr_blob_index);
			get_value_from_element_from_tuple(&curr_blob, wri_p->wtd_p->partial_blob_tuple_def, SELF, blob_tuple);
		}

		// compute bytes_readable
		uint32_t bytes_readable = min(data_size, curr_blob.blob_size - wri_p->curr_byte_index);

		// move the data and data_size buffer references forward by bytes_readable
		// and read respective data if data != NULL
		if(data != NULL)
		{
			memory_move(data, curr_blob.blob_value + wri_p->curr_byte_index, bytes_readable);
			data += bytes_readable;
		}
		data_size -= bytes_readable;

		// update the return value
		bytes_read += bytes_readable;

		// move forward the iterator by bytes_readable bytes
		wri_p->curr_byte_index += bytes_readable;
		if(wri_p->curr_byte_index == curr_blob.blob_size)
		{
			wri_p->curr_blob_index++;
			wri_p->curr_byte_index = 0;
		}
	}

	return bytes_read;
}

const char* peek_in_worm(worm_read_iterator* wri_p, uint32_t* data_size, const void* transaction_id, int* abort_error)
{
	const char* data = NULL;
	(*data_size) = 0;

	while((*data_size) == 0)
	{
		// if you are blob_index is out of bounds, then change the page
		if(wri_p->curr_blob_index >= get_tuple_count_on_persistent_page(&(wri_p->curr_page), wri_p->wtd_p->pas_p->page_size, &(wri_p->wtd_p->partial_blob_tuple_def->size_def)))
		{
			// if there is no next page to go to, then break out of the loop
			if(!can_goto_next_page(wri_p))
				break;

			// goto the next page
			goto_next_page(wri_p, transaction_id, abort_error);
			if(*abort_error)
				return 0;

			wri_p->curr_blob_index = 0;
			wri_p->curr_byte_index = 0;
		}

		// get the curr_blob, the tuple at curr_blob_index
		user_value curr_blob;
		{
			const void* blob_tuple = get_nth_tuple_on_persistent_page(&(wri_p->curr_page), wri_p->wtd_p->pas_p->page_size, &(wri_p->wtd_p->partial_blob_tuple_def->size_def), wri_p->curr_blob_index);
			get_value_from_element_from_tuple(&curr_blob, wri_p->wtd_p->partial_blob_tuple_def, SELF, blob_tuple);
		}

		// compute bytes_readable
		uint32_t bytes_readable = (curr_blob.blob_size - wri_p->curr_byte_index);

		// if there are no bytes peekable, then go to the next blob
		if(bytes_readable == 0)
		{
			wri_p->curr_blob_index++;
			wri_p->curr_byte_index = 0;
			continue; // this continue ensures that the next blob will not be out-of-bounds on this very page, if so we will goto next page first blob in the next page
		}

		data = curr_blob.blob_value + wri_p->curr_byte_index;
		(*data_size) = bytes_readable; // we can break out of the loop here, only if the data_size is non-zero
	}

	return data;
}

void delete_worm_read_iterator(worm_read_iterator* wri_p, const void* transaction_id, int* abort_error)
{
	// if curr_page is still locked, then release this lock
	// it may not be locked, if we encountered an abort error
	if(!is_persistent_page_NULL(&(wri_p->curr_page), wri_p->pam_p))
		release_lock_on_persistent_page(wri_p->pam_p, transaction_id, &(wri_p->curr_page), NONE_OPTION, abort_error);
	free(wri_p);
}