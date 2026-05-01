#include<tupleindexer/blob_store/blob_store_chunk_util.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<tuplestore/tuple.h>
#include<tuplestore/datum.h>

void initialize_chunk(void* chunk, const void* chunk_data, uint32_t chunk_data_size, uint64_t next_page_id, uint32_t next_tuple_index, const blob_store_tuple_defs* bstd_p)
{
	init_tuple(bstd_p->chunk_tuple_def, chunk);

	set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk, &(const datum){.binary_value = chunk_data, .binary_size = chunk_data_size}, UINT32_MAX);

	set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(1), chunk, &(const datum){.uint_value = next_page_id}, 0);
	set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(2), chunk, &(const datum){.uint_value = next_tuple_index}, 0);
}

uint64_t get_next_chunk_pointer(const void* chunk, uint32_t* next_tuple_index, const blob_store_tuple_defs* bstd_p)
{
	datum uval;

	get_value_from_element_from_tuple(&uval, bstd_p->chunk_tuple_def, STATIC_POSITION(2), chunk);
	(*next_tuple_index) = uval.uint_value;

	get_value_from_element_from_tuple(&uval, bstd_p->chunk_tuple_def, STATIC_POSITION(1), chunk);
	return uval.uint_value;
}

void set_next_chunk_pointer(void* chunk, uint64_t next_page_id, uint32_t next_tuple_index, const blob_store_tuple_defs* bstd_p)
{
	set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(1), chunk, &(const datum){.uint_value = next_page_id}, 0);
	set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(2), chunk, &(const datum){.uint_value = next_tuple_index}, 0);
}

uint32_t append_bytes_to_back_of_chunk(void* chunk, void* data, uint32_t data_size, uint32_t max_size_increment_allowed, const blob_store_tuple_defs* bstd_p);

uint32_t discard_bytes_from_front_of_chunk(void* chunk, uint32_t data_size, const blob_store_tuple_defs* bstd_p)
{
	uint32_t chunk_data_size = get_element_count_for_element_from_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk);

	uint32_t bytes_discarded = min(data_size, chunk_data_size);

	discard_elements_from_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk, 0, bytes_discarded);

	return bytes_discarded;
}

int set_next_chunk_pointer_in_ppage(persistent_page* ppage, uint32_t tuple_index, uint64_t next_page_id, uint32_t next_tuple_index, const blob_store_tuple_defs* bstd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	set_element_in_tuple_in_place_on_persistent_page(pmm_p, transaction_id, ppage, bstd_p->pas_p->page_size, bstd_p->chunk_tuple_def, tuple_index, STATIC_POSITION(1), &(const datum){.uint_value = next_page_id}, abort_error);
	if(*abort_error)
		return 0;

	set_element_in_tuple_in_place_on_persistent_page(pmm_p, transaction_id, ppage, bstd_p->pas_p->page_size, bstd_p->chunk_tuple_def, tuple_index, STATIC_POSITION(2), &(const datum){.uint_value = next_tuple_index}, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}