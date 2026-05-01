#ifndef BLOB_STORE_READ_ITERATOR_H
#define BLOB_STORE_READ_ITERATOR_H

#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/blob_store/blob_store_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>

typedef struct blob_store_read_iterator blob_store_read_iterator;
struct blob_store_read_iterator
{
	// curr_page that we are looking at
	persistent_page curr_page;

	// curr_tuple_index -> index of the tuple we are looking at in curr_page
	uint32_t curr_tuple_index;

	// curr_byte_index -> index of the byte in the binary of the tuple that we are looking at
	uint32_t curr_byte_index;

	const blob_store_tuple_defs* bstd_p;

	const page_access_methods* pam_p;
};

// all functions on blob_store_read_iterator are declared here, in this header file
#include<tupleindexer/blob_store/blob_store_read_iterator_public.h>

#endif