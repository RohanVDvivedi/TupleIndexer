#include<tupleindexer/blob_store/blob_store_write_iterator.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<tupleindexer/blob_store/blob_store_chunk_util.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<stdlib.h>

blob_store_write_iterator* get_new_blob_store_write_iterator(uint64_t root_page_id, uint64_t head_page_id, uint32_t head_tuple_index, uint64_t tail_page_id, uint32_t tail_tuple_index, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 3 must be present
	if(bstd_p == NULL || pam_p == NULL || pmm_p == NULL)
		return NULL;

	// allocate enough memory
	blob_store_write_iterator* bswi_p = malloc(sizeof(blob_store_write_iterator));
	if(bswi_p == NULL)
		exit(-1);

	bswi_p->root_page_id = root_page_id;

	bswi_p->head_page_id = head_page_id;
	bswi_p->head_tuple_index = head_tuple_index;

	bswi_p->tail_page_id = tail_page_id;
	bswi_p->tail_tuple_index = tail_tuple_index;

	bswi_p->tail_byte_index = 0;
	bswi_p->is_tail_page_full = 0;

	// if there is tail_page_id provided, compute tail_byte_index and is_tail_page_full
	if(tail_page_id != bstd_p->pas_p->NULL_PAGE_ID)
	{
		persistent_page tail_page = acquire_persistent_page_with_lock(pam_p, transaction_id, tail_page_id, READ_LOCK, abort_error);
		if(*abort_error)
		{
			free(bswi_p);
			return NULL;
		}

		{
			const void* tail_chunk = get_nth_tuple_on_persistent_page(&tail_page, bstd_p->pas_p->page_size, &(bstd_p->chunk_tuple_def->size_def), tail_tuple_index);
			if(tail_chunk == NULL)
			{
				free(bswi_p);
				return NULL;
			}

			datum tail_chunk_data = get_curr_chunk_data(tail_chunk, bstd_p);
			if(is_datum_NULL(&tail_chunk_data))
			{
				free(bswi_p);
				return NULL;
			}

			bswi_p->tail_byte_index = tail_chunk_data.binary_size;
		}

		{
			uint32_t bytes_appendable_on_tail_page = get_space_allotted_to_all_tuples_on_persistent_page(&tail_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def))
				+ get_space_occupied_by_all_tuples_on_persistent_page(&tail_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def));
			bswi_p->is_tail_page_full = (bytes_appendable_on_tail_page == 0);
		}

		release_lock_on_persistent_page(pam_p, transaction_id, &tail_page, NONE_OPTION, abort_error);
		if(*abort_error)
		{
			free(bswi_p);
			return NULL;
		}
	}

	bswi_p->bstd_p = bstd_p;
	bswi_p->pam_p = pam_p;
	bswi_p->pmm_p = pmm_p;

	return bswi_p;
}

int is_empty_blob_in_blob_store(const blob_store_write_iterator* bswi_p)
{
	return (bswi_p->head_page_id == bswi_p->bstd_p->pas_p->NULL_PAGE_ID) && (bswi_p->tail_page_id == bswi_p->bstd_p->pas_p->NULL_PAGE_ID);
}

uint64_t get_head_position_in_blob(const blob_store_write_iterator* bswi_p, uint32_t* head_tuple_index)
{
	(*head_tuple_index) = bswi_p->head_tuple_index;
	return bswi_p->head_page_id;
}

uint64_t get_tail_position_in_blob(const blob_store_write_iterator* bswi_p, uint32_t* tail_tuple_index, uint32_t* tail_byte_index)
{
	(*tail_byte_index) = bswi_p->tail_byte_index;
	(*tail_tuple_index) = bswi_p->tail_tuple_index;
	return bswi_p->tail_page_id;
}

uint32_t append_to_tail_in_blob(blob_store_write_iterator* bswi_p, const heap_table_notifier* notify_wrong_entry, const char* data, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	// no bytes to append
	if(data_size == 0)
		return 0;

	int was_empty = is_empty_blob_in_blob_store(bswi_p);

	// if the blob is not empty, then tail must exists, if not we fail
	if(!was_empty)
		if(bswi_p->tail_page_id == bswi_p->bstd_p->pas_p->NULL_PAGE_ID)
			return 0;

	uint32_t bytes_appended = 0;

	persistent_page old_tail_page = get_NULL_persistent_page(bswi_p->pam_p);
	persistent_page tail_page = get_NULL_persistent_page(bswi_p->pam_p);

	// append to existing tail_chunk
	if((bytes_appended == 0) && (bswi_p->tail_page_id != bswi_p->bstd_p->pas_p->NULL_PAGE_ID) && (!(bswi_p->is_tail_page_full)))
	{
		// get write lock on the tail page
		old_tail_page = acquire_persistent_page_with_lock(bswi_p->pam_p, transaction_id, bswi_p->tail_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// get the actual unused_space on the page
		uint32_t bytes_appendable = get_space_allotted_to_all_tuples_on_persistent_page(&old_tail_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def))
			- get_space_occupied_by_all_tuples_on_persistent_page(&old_tail_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def));

		if(bytes_appendable == 0)
		{
			// update the hint
			bswi_p->is_tail_page_full = 1;
		}
		else
		{
			// get the tail_chunk
			const void* tail_chunk = get_nth_tuple_on_persistent_page(&old_tail_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def), bswi_p->tail_tuple_index);

			bytes_appended = min(bytes_appendable, data_size);

			// clone the tail_chunk
			uint32_t tail_chunk_size = get_tuple_size(bswi_p->bstd_p->chunk_tuple_def, tail_chunk) + bytes_appended;
			void* cloned_tail_chunk = malloc(tail_chunk_size);
			memory_move(cloned_tail_chunk, tail_chunk, tail_chunk_size);

			// append to it bytes_appended number of bytes
			append_bytes_to_back_of_chunk(cloned_tail_chunk, data, bytes_appended, bytes_appended, bswi_p->bstd_p);

			// update the tail
			update_tuple_on_persistent_page_resiliently(bswi_p->pmm_p, transaction_id, &old_tail_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def), bswi_p->tail_tuple_index, cloned_tail_chunk, abort_error);
			free(cloned_tail_chunk);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &old_tail_page, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// if nothing was appended, we need to add a new chunk
	if(bytes_appended == 0)
	{
		uint64_t old_tail_page_id = bswi_p->tail_page_id;
		uint64_t old_tail_tuple_index = bswi_p->tail_tuple_index;

		uint64_t tail_page_id;
		uint64_t tail_tuple_index;

		if(bytes_appended == 0 && data_size < bswi_p->bstd_p->max_data_bytes_in_chunk) // find a page with enough free space to insert the chunk in it
		{
			uint32_t unused_space_in_entry;

			uint32_t required_unused_space = bswi_p->bstd_p->min_chunk_size - 1 + data_size;

			tail_page = find_heap_page_with_enough_unused_space_from_heap_table(bswi_p->root_page_id, required_unused_space, &unused_space_in_entry, notify_wrong_entry, &(bswi_p->bstd_p->httd), bswi_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// move further only if a page with large enough space is found
			if(!is_persistent_page_NULL(&tail_page, bswi_p->pam_p))
			{
				tail_page_id = tail_page.page_id;

				bytes_appended = data_size;

				// make chunk large enough to accomodate the remaining bytes
				void* chunk = malloc(bswi_p->bstd_p->max_chunk_size);
				initialize_chunk(chunk, data, bytes_appended, bswi_p->bstd_p->pas_p->NULL_PAGE_ID, 0, bswi_p->bstd_p);

				// insert it
				uint32_t possible_insertion_index = 0;
				tail_tuple_index = insert_in_heap_page(&tail_page, chunk, &possible_insertion_index, bswi_p->bstd_p->chunk_tuple_def, bswi_p->bstd_p->pas_p, bswi_p->pmm_p, transaction_id, abort_error);
				free(chunk);
				if(*abort_error)
					goto ABORT_ERROR;

				// notify that the unused space changed
				if(notify_wrong_entry)
					notify_wrong_entry->notify(notify_wrong_entry->context, bswi_p->root_page_id, unused_space_in_entry, tail_page_id);

				release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &tail_page, NONE_OPTION, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}
		}

		// if still no success (bytes_appended == 0), allocate a new page for the new tail chunk

		if(bytes_appended == 0) // add a new full page as a new chunk having the only chunk as it's only tuple
		{
			// this new page will only accomodate the new chunk
			bytes_appended = bswi_p->bstd_p->max_data_bytes_in_chunk;

			// grab a new tail_page
			tail_page = get_new_heap_page_with_write_lock(bswi_p->bstd_p->pas_p, bswi_p->bstd_p->chunk_tuple_def, bswi_p->pam_p, bswi_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			tail_page_id = tail_page.page_id;

			// make chunk large enough to fit the page
			void* chunk = malloc(bswi_p->bstd_p->max_chunk_size);
			initialize_chunk(chunk, data, bytes_appended, bswi_p->bstd_p->pas_p->NULL_PAGE_ID, 0, bswi_p->bstd_p);

			uint32_t possible_insertion_index = 0;
			tail_tuple_index = insert_in_heap_page(&tail_page, chunk, &possible_insertion_index, bswi_p->bstd_p->chunk_tuple_def, bswi_p->bstd_p->pas_p, bswi_p->pmm_p, transaction_id, abort_error);
			free(chunk);
			if(*abort_error)
				goto ABORT_ERROR;

			// make new tail_page to be tracked by the heap_table btree for its unused_space, eventhough it has none
			track_unused_space_in_heap_table(bswi_p->root_page_id, &tail_page, &(bswi_p->bstd_p->httd), bswi_p->pam_p, bswi_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &tail_page, NONE_OPTION, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// update bswi_p's tail pointer
		bswi_p->tail_page_id = tail_page_id;
		bswi_p->tail_tuple_index = tail_tuple_index;

		// now update the pointer on the old tail chunk

		// get write lock on the tail page
		old_tail_page = acquire_persistent_page_with_lock(bswi_p->pam_p, transaction_id, old_tail_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		set_next_chunk_pointer_in_ppage(&old_tail_page, old_tail_tuple_index, tail_page_id, tail_tuple_index, bswi_p->bstd_p, bswi_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &old_tail_page, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// the the blob was earlier was empty, we need to make the head and tail point to the same only chunk
	if(was_empty)
	{
		bswi_p->head_page_id = bswi_p->tail_page_id;
		bswi_p->head_tuple_index = bswi_p->tail_tuple_index;
	}

	return bytes_appended;

	ABORT_ERROR:
	if(!is_persistent_page_NULL(&tail_page, bswi_p->pam_p))
		release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &tail_page, NONE_OPTION, abort_error);
	if(!is_persistent_page_NULL(&old_tail_page, bswi_p->pam_p))
		release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &old_tail_page, NONE_OPTION, abort_error);
	return 0;
}

uint32_t discard_from_head_in_blob(blob_store_write_iterator* bswi_p, const heap_table_notifier* notify_wrong_entry, uint32_t data_size, const void* transaction_id, int* abort_error)
{
	// no bytes to discard
	if(data_size == 0)
		return 0;

	// can not discard if the blob is empty
	if(is_empty_blob_in_blob_store(bswi_p))
		return 0;

	// can not discard if the head is not present
	if(bswi_p->head_page_id == bswi_p->bstd_p->pas_p->NULL_PAGE_ID)
		return 0;

	// get write lock on the head page
	persistent_page head_page = acquire_persistent_page_with_lock(bswi_p->pam_p, transaction_id, bswi_p->head_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		return 0;

	// must not return NULL, this tuple must exist
	const void* head_chunk = get_nth_tuple_on_persistent_page(&head_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def), bswi_p->head_tuple_index);

	// get the number of bytes in head_chunk
	uint32_t data_bytes_in_head_chunk = 0;
	{
		datum head_chunk_data = get_curr_chunk_data(head_chunk, bswi_p->bstd_p);
		if(!is_datum_NULL(&head_chunk_data))
			data_bytes_in_head_chunk = head_chunk_data.binary_size;
	}

	uint32_t bytes_discarded = 0;

	if(data_bytes_in_head_chunk > data_size) // shrink the chunk
	{
		// clone the head_chunk
		uint32_t head_chunk_size = get_tuple_size(bswi_p->bstd_p->chunk_tuple_def, head_chunk);
		void* cloned_head_chunk = malloc(head_chunk_size);
		memory_move(cloned_head_chunk, head_chunk, head_chunk_size);

		// discard bytes from its (clone's) front
		discard_bytes_from_front_of_chunk(cloned_head_chunk, data_bytes_in_head_chunk, bswi_p->bstd_p);

		// update this head_chunk with it's clone
		update_tuple_on_persistent_page_resiliently(bswi_p->pmm_p, transaction_id, &head_page, bswi_p->bstd_p->pas_p->page_size, &(bswi_p->bstd_p->chunk_tuple_def->size_def), bswi_p->head_tuple_index, cloned_head_chunk, abort_error);
		free(cloned_head_chunk);
		if(*abort_error)
			goto ABORT_ERROR;

		bytes_discarded = data_size;
	}
	else // discard the chunk
	{
		uint64_t old_head_page_id = bswi_p->head_page_id;
		uint32_t old_head_tuple_index = bswi_p->head_tuple_index;

		uint32_t old_head_page_unused_space = get_unused_space_on_heap_page(&head_page, bswi_p->bstd_p->pas_p, bswi_p->bstd_p->chunk_tuple_def);

		// fetch the new head
		bswi_p->head_page_id = get_next_chunk_pointer(head_chunk, &(bswi_p->head_tuple_index), bswi_p->bstd_p);

		// discard the chunk
		delete_from_heap_page(&head_page, old_head_tuple_index, bswi_p->bstd_p->chunk_tuple_def, bswi_p->bstd_p->pas_p, bswi_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// notify that the unused space changed
		if(notify_wrong_entry)
			notify_wrong_entry->notify(notify_wrong_entry->context, bswi_p->root_page_id, old_head_page_unused_space, old_head_page_id);

		bytes_discarded = data_bytes_in_head_chunk;

		// if we destroyed the only chunk, make the tail also NULL_PAGE_ID
		// i.e. reset tail pointer if the head_page_id just became NULL, making the blob empty
		if(bswi_p->head_page_id == bswi_p->bstd_p->pas_p->NULL_PAGE_ID)
		{
			bswi_p->tail_page_id = bswi_p->bstd_p->pas_p->NULL_PAGE_ID;
			bswi_p->tail_tuple_index = 0;

			bswi_p->tail_byte_index = 0;
			bswi_p->is_tail_page_full = 0;
		}
	}

	// release the lock and exit
	release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return bytes_discarded;

	ABORT_ERROR:
	if(!is_persistent_page_NULL(&head_page, bswi_p->pam_p))
		release_lock_on_persistent_page(bswi_p->pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
	return 0;
}

void delete_blob_store_write_iterator(blob_store_write_iterator* bswi_p, const void* transaction_id, int* abort_error)
{
	free(bswi_p);
}