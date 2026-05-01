#ifndef BLOB_STORE_TUPLE_DEFINITIONS_PUBLIC_H
#define BLOB_STORE_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuplestore/tuple.h>
#include<inttypes.h>

#include<tupleindexer/heap_table/heap_table_tuple_definitions_public.h>

#include<tupleindexer/common/page_access_specification.h>

typedef struct blob_store_tuple_defs blob_store_tuple_defs;
struct blob_store_tuple_defs
{
	// specification of all the pages in the blob_store
	const page_access_specs* pas_p;

	// tuple_def for all chunks in the blob_store
	// contains variable_length_binary + next_page_id + next_tuple_index
	tuple_def* chunk_tuple_def;

	uint32_t min_chunk_size; // chunk with just 1 byte in it
	uint32_t max_chunk_size; // chunk with it being the only one on the page and occupying all of the space on that heap_page

	uint32_t max_data_bytes_in_chunk; // max_chunk_size - min_chunk_size + 1, maximum number of raw bytes we can put in chunk

	// the tuple_defs for the heap_table that helps us manager free space for the chunks
	heap_table_tuple_defs httd;
};

// initializes the attributes in blob_store_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_blob_store_tuple_definitions(blob_store_tuple_defs* bstd_p, const page_access_specs* pas_p);

// then resets all the blob_store_tuple_defs struct attributes to NULL or 0
void deinit_blob_store_tuple_definitions(blob_store_tuple_defs* bstd_p);

// print blob_store_tuple_defs
void print_blob_store_tuple_definitions(const blob_store_tuple_defs* bstd_p);

#endif