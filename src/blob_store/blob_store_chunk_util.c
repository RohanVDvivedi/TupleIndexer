#include<tupleindexer/blob_store/blob_store_chunk_util.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<tuplestore/tuple.h>
#include<tuplestore/datum.h>

void initialize_chunk(void* chunk, const void* chunk_data, uint32_t chunk_data_size, tuple_pointer next_chunk_pointer, const blob_store_tuple_defs* bstd_p)
{
	init_tuple(bstd_p->chunk_tuple_def, chunk);

	set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk, &(const datum){.binary_value = chunk_data, .binary_size = chunk_data_size}, UINT32_MAX);

	{
		char next_chunk_pointer_tuple[sizeof(tuple_pointer)];
		set_tuple_pointer(next_chunk_pointer_tuple, next_chunk_pointer, bstd_p->pas_p);
		set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(1), chunk, &(const datum){.tuple_value = next_chunk_pointer_tuple}, 0);
	}
}

tuple_pointer get_next_chunk_pointer(const void* chunk, const blob_store_tuple_defs* bstd_p)
{
	datum uval;
	get_value_from_element_from_tuple(&uval, bstd_p->chunk_tuple_def, STATIC_POSITION(1), chunk);
	return get_tuple_pointer(uval.tuple_value, bstd_p->pas_p);
}

datum get_curr_chunk_data(const void* chunk, const blob_store_tuple_defs* bstd_p)
{
	datum chunk_data;
	if(!get_value_from_element_from_tuple(&chunk_data, bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk))
		return (*NULL_DATUM);
	return chunk_data;
}

void set_next_chunk_pointer(void* chunk, tuple_pointer next_chunk_pointer, const blob_store_tuple_defs* bstd_p)
{
	{
		char next_chunk_pointer_tuple[sizeof(tuple_pointer)];
		set_tuple_pointer(next_chunk_pointer_tuple, next_chunk_pointer, bstd_p->pas_p);
		set_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(1), chunk, &(const datum){.tuple_value = next_chunk_pointer_tuple}, 0);
	}
}

uint32_t append_bytes_to_back_of_chunk(void* chunk, const void* data, uint32_t data_size, uint32_t max_size_increment_allowed, const blob_store_tuple_defs* bstd_p)
{
	uint32_t bytes_appended = min(data_size, min(get_max_size_increment_allowed_for_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk), max_size_increment_allowed));

	uint32_t old_chunk_data_size = get_element_count_for_element_from_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk);

	// expand the chunk_data, this must succeed
	expand_element_count_for_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk, old_chunk_data_size, bytes_appended, max_size_increment_allowed);

	// this copy is illegal, ideally a set should be called, but this is faster
	{
		datum uval;
		get_value_from_element_from_tuple(&uval, bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk);
		memory_move(((char*)uval.binary_value) + old_chunk_data_size, data, bytes_appended);
	}

	return bytes_appended;
}

uint32_t discard_bytes_from_front_of_chunk(void* chunk, uint32_t data_size, const blob_store_tuple_defs* bstd_p)
{
	uint32_t chunk_data_size = get_element_count_for_element_from_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk);

	uint32_t bytes_discarded = min(data_size, chunk_data_size);

	discard_elements_from_element_in_tuple(bstd_p->chunk_tuple_def, STATIC_POSITION(0), chunk, 0, bytes_discarded);

	return bytes_discarded;
}

int set_next_chunk_pointer_in_ppage(persistent_page* ppage, uint32_t tuple_index, tuple_pointer next_chunk_pointer, const blob_store_tuple_defs* bstd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	char next_chunk_pointer_tuple[sizeof(tuple_pointer)];
	set_tuple_pointer(next_chunk_pointer_tuple, next_chunk_pointer, bstd_p->pas_p);

	set_element_in_tuple_in_place_on_persistent_page(pmm_p, transaction_id, ppage, bstd_p->pas_p->page_size, bstd_p->chunk_tuple_def, tuple_index, STATIC_POSITION(1), &(const datum){.tuple_value = next_chunk_pointer_tuple}, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}