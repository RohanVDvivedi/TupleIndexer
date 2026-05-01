#ifndef BLOB_STORE_WRITE_ITERATOR_H
#define BLOB_STORE_WRITE_ITERATOR_H

#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/blob_store/blob_store_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

typedef struct blob_store_write_iterator blob_store_write_iterator;
struct blob_store_write_iterator
{
	uint64_t head_page_id;
	uint32_t head_tuple_index;

	uint64_t tail_page_id;
	uint32_t tail_tuple_index;

	// starts with being 0, and gets set/reset after every append
	// if 0, we try to append to the tail chunk
	int is_tail_page_full;

	const blob_store_tuple_defs* bstd_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
};

// all functions on blob_store_write_iterator are declared here, in this header file
#include<tupleindexer/blob_store/blob_store_write_iterator_public.h>

#endif