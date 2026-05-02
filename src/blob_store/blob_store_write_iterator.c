#include<tupleindexer/blob_store/blob_store_write_iterator.h>

#include<stdlib.h>

blob_store_write_iterator* get_new_blob_store_write_iterator(uint64_t root_page_id, uint64_t head_page_id, uint32_t head_tuple_index, uint64_t tail_page_id, uint32_t tail_tuple_index, const blob_store_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

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