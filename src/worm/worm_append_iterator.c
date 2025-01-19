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

uint32_t append_to_worm(worm_append_iterator* wai_p, const char* data, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	// no data to append is always a success
	if(data_size == 0)
		return 0;

	char* blob_tuple_buffer = malloc(wai_p->wtd_p->pas_p->page_size);
	if(blob_tuple_buffer == NULL)
		exit(-1);

	// persistent page for locking tail page, if tail page is not the head page
	persistent_page tail_page = get_NULL_persistent_page(wai_p->pam_p);

	// this pointer may point to wai_p->head_page, if head is the tail page, else it will point to the local variable above
	persistent_page* tail_page_p = NULL;

	// grab the tail_page_id from the head page
	uint64_t tail_page_id = get_tail_page_id_of_worm_head_page(&(wai_p->head_page), wai_p->wtd_p);

	if(tail_page_id == wai_p->head_page.page_id) // if head_page is the tail page
		tail_page_p = &(wai_p->head_page);
	else // else we need to lock the tail
	{
		tail_page_p = &tail_page;
		(*tail_page_p) = acquire_persistent_page_with_lock(wai_p->pam_p, transaction_id, tail_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// now we can directly use the tail_page_p to access tail

	uint32_t bytes_appended = 0;

	while(data_size > 0)
	{
		uint32_t bytes_appendable = 0;
		while((bytes_appendable = blob_bytes_appendable_on_worm_page(tail_page_p, wai_p->wtd_p)) == 0) // keep on appending new tail pages, while there are no appendable bytes on that page, ideally this loop should run just once
		{
			// create a new tail page
			persistent_page new_page = get_new_persistent_page_with_write_lock(wai_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// init it as any other page, it is surely not the head page
			init_worm_any_page(&new_page, wai_p->wtd_p, wai_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// make next_page_id of tail_p point to new_page
			{
				update_next_page_id_on_worm_page(tail_page_p, new_page.page_id, wai_p->wtd_p, wai_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
					goto ABORT_ERROR;
				}
			}

			// make tail_page_id of head_page point to new_page
			{
				worm_head_page_header hdr = get_worm_head_page_header(&(wai_p->head_page), wai_p->wtd_p);
				hdr.tail_page_id = new_page.page_id;
				set_worm_head_page_header(&(wai_p->head_page), &hdr, wai_p->wtd_p, wai_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
					goto ABORT_ERROR;
				}
			}

			// make new_page the new tail_page in the local context

			// for this release any lock on the tail_page if any is held
			if(is_persistent_page_NULL(&tail_page, wai_p->pam_p))
			{
				release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &tail_page, NONE_OPTION, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
					goto ABORT_ERROR;
				}
			}
			tail_page_p = &tail_page; // new_page is surely not a head page so use the tail_page local variable
			(*tail_page_p) = new_page;
		}

		// you can not append more bytes than what the data currently points to
		bytes_appendable = min(bytes_appendable, data_size);

		// build user value for the blob
		user_value uval = {.blob_value = data, .blob_size = bytes_appendable};

		// build tuple in blob_tuple_buffer for the blob
		init_tuple(wai_p->wtd_p->partial_blob_tuple_def, blob_tuple_buffer);
		set_element_in_tuple(wai_p->wtd_p->partial_blob_tuple_def, SELF, blob_tuple_buffer, &uval, UINT32_MAX);

		// append blob_tuple_buffer onto the page
		append_tuple_on_persistent_page_resiliently(wai_p->pmm_p, transaction_id, tail_page_p, wai_p->wtd_p->pas_p->page_size, &(wai_p->wtd_p->partial_blob_tuple_def->size_def), blob_tuple_buffer, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		bytes_appended += bytes_appendable;
		data += bytes_appendable;
		data_size -= bytes_appendable;
	}

	// only tail_page could be locked, in thelocal scope, if you read here
	// if it is locked release it
	if(is_persistent_page_NULL(&tail_page, wai_p->pam_p))
	{
		release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &tail_page, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	free(blob_tuple_buffer);

	return bytes_appended;

	ABORT_ERROR:;
	// in case of abort, release lock on head_page and tail_page (if tail_page is not NULL)
	if(is_persistent_page_NULL(&tail_page, wai_p->pam_p))
		release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &tail_page, NONE_OPTION, abort_error);
	free(blob_tuple_buffer);
	release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &(wai_p->head_page), NONE_OPTION, abort_error);
	return 0;
}

void delete_worm_append_iterator(worm_append_iterator* wai_p, const void* transaction_id, int* abort_error)
{
	// if head_page is still locked, then release this lock
	// it may not be locked, if we encountered an abort error
	if(!is_persistent_page_NULL(&(wai_p->head_page), wai_p->pam_p))
		release_lock_on_persistent_page(wai_p->pam_p, transaction_id, &(wai_p->head_page), NONE_OPTION, abort_error);
	free(wai_p);
}