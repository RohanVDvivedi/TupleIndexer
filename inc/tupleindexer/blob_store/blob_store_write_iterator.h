#ifndef BLOB_STORE_WRITE_ITERATOR_H
#define BLOB_STORE_WRITE_ITERATOR_H

#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/blob_store/blob_store_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

typedef struct blob_store_write_iterator blob_store_write_iterator;
struct blob_store_write_iterator
{
	uint64_t root_page_id;

	uint64_t head_page_id;
	uint32_t head_tuple_index;

	uint64_t tail_page_id;
	uint32_t tail_tuple_index;

	const blob_store_tuple_defs* bstd_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
};

// all functions on blob_store_write_iterator are declared here, in this header file
#include<tupleindexer/blob_store/blob_store_write_iterator_public.h>

#endif