#ifndef LINKED_PAGE_LIST_ITERATOR_H
#define LINKED_PAGE_LIST_ITERATOR_H

#include<persistent_page.h>
#include<linked_page_list_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

typedef struct persistent_page_reference persistent_page_reference;
struct persistent_page_reference
{
	int points_to_iterator_head;	// if set to 1, then the linked_page_list_iterator's head_page is what this reference points to
	persistent_page non_head_page;	// else, this is what it will point to.
};

typedef struct linked_page_list_iterator linked_page_list_iterator;
struct linked_page_list_iterator
{
	// current page that we are pointing to
	persistent_page_reference curr_page;

	// tuple we are pointing at
	uint32_t curr_tuple_index;

	// head_page never changes and is never freed, this is the head_page of the linked_page_list
	persistent_page head_page;

	const linked_page_list_tuple_defs* lpltd_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
	// for a read-only linked_page_list_iterator, pmm_p = NULL
};

#include<linked_page_list_iterator_public.h>

#endif