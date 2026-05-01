#ifndef BLOB_STORE_H
#define BLOB_STORE_H

#include<tupleindexer/blob_store/blob_store_tuple_definitions_public.h>

#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

/*
	Blob store is a simple tool to store your larger than page blobs as chunks in singly linked list, linked with next_page_id and next_tuple_index

	it is basically a heap_table of blobs that has it's chunks, linked by next_page_id and next_tuple_index, and has stable pointers for these chunks
*/

// returns pointer to the root page of the newly created heap_table for blob_store
uint64_t get_new_blob_store(const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the blob_store
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given blob_store
int destroy_blob_store(uint64_t root_page_id, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the chunks in the blob_store, for all the blobs
void print_blob_store(uint64_t root_page_id, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// a read utility to get the current maximum level this blob_store hosts, this can be used to approximate the number of buffer pages required
uint32_t get_root_level_blob_store(uint64_t root_page_id, const blob_store_tuple_defs* bstd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#include<tupleindexer/blob_store/blob_store_read_iterator_public.h>
#include<tupleindexer/blob_store/blob_store_write_iterator_public.h>

#endif