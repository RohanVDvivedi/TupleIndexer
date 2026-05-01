#ifndef BLOB_STORE_CHUNK_UTIL_H
#define BLOB_STORE_CHUNK_UTIL_H

#include<tupleindexer/blob_store/blob_store_tuple_definitions.h>

#include<tupleindexer/utils/persistent_page.h>

#include<tupleindexer/interface/opaque_page_modification_methods.h>

void initialize_chunk(void* chunk, const void* chunk_data, uint32_t chunk_data_size, uint64_t next_page_id, uint32_t next_tuple_index, const blob_store_tuple_defs* bstd_p);

uint64_t get_next_chunk_pointer(const void* chunk, uint32_t* next_tuple_index, const blob_store_tuple_defs* bstd_p);

void set_next_chunk_pointer(void* chunk, uint64_t next_page_id, uint32_t next_tuple_index, const blob_store_tuple_defs* bstd_p);

// max_size_increment_allowed is the number of unused bytes on the page
// number of appended bytes are returned
uint32_t append_bytes_to_back_of_chunk(void* chunk, void* data, uint32_t data_size, uint32_t max_size_increment_allowed, const blob_store_tuple_defs* bstd_p);

// atmost data_size number of bytes are discarded
// number of bytes discarded are returned
uint32_t discard_bytes_from_front_of_chunk(void* chunk, uint32_t data_size, const blob_store_tuple_defs* bstd_p);

int set_next_chunk_pointer_in_ppage(persistent_page* ppage, uint32_t tuple_index, uint64_t next_page_id, uint32_t next_tuple_index, const blob_store_tuple_defs* bstd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif