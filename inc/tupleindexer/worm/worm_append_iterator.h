#ifndef WORM_APPEND_ITERATOR_H
#define WORM_APPEND_ITERATOR_H

#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/worm/worm_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

typedef struct worm_append_iterator worm_append_iterator;
struct worm_append_iterator
{
	persistent_page head_page;

	const worm_tuple_defs* wtd_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
};

// all functions on worm_append_iterator are declared here, in this header file
#include<tupleindexer/worm/worm_append_iterator_public.h>

#endif