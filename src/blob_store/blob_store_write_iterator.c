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
			bswi_p->is_tail_page_full = (bstd_p->min_chunk_size > get_unused_space_on_heap_page(&tail_page, bstd_p->pas_p, bstd_p->chunk_tuple_def));
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

uint32_t append_to_tail_in_blob(blob_store_write_iterator* bswi_p, const char* data, uint32_t data_size, const void* transaction_id, int* abort_error);

uint32_t discard_from_head_in_blob(blob_store_write_iterator* bswi_p, uint32_t data_size, const void* transaction_id, int* abort_error);

void delete_blob_store_write_iterator(blob_store_write_iterator* bswi_p, const void* transaction_id, int* abort_error)
{
	free(bswi_p);
}