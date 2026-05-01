#include<tupleindexer/blob_store/blob_store_read_iterator.h>

uint64_t get_position_in_blob(const blob_store_read_iterator* bsri_p, uint32_t* curr_tuple_index, uint32_t* curr_byte_index)
{
	(*curr_byte_index) = bsri_p->curr_byte_index;
	(*curr_tuple_index) = bsri_p->curr_tuple_index;
	return bsri_p->curr_page.page_id;
}