#include<tupleindexer/blob_store/blob_store_write_iterator.h>

#include<stdlib.h>

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

void delete_blob_store_write_iterator(blob_store_write_iterator* bswi_p, const void* transaction_id, int* abort_error)
{
	free(bswi_p);
}