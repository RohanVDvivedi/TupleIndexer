#ifndef WORM_READ_ITERATOR_H
#define WORM_READ_ITERATOR_H

#include<persistent_page.h>
#include<worm_tuple_definitions.h>
#include<opaque_page_access_methods.h>

typedef struct worm_append_iterator worm_append_iterator;
struct worm_append_iterator
{
	// curr_page that we are looking at
	persistent_page curr_page;

	// curr_blob_index -> index of the tuple we are looking at in curr_page
	uint32_t curr_blob_index;

	// curr_byte_index -> index of the byte in the blob that we are looking at
	uint32_t curr_byte_index;

	const worm_tuple_defs* wtd_p;

	const page_access_methods* pam_p;
};

// all functions on worm_read_iterator are declared here, in this header file
#include<worm_read_iterator_public.h>

#endif