#ifndef LINKED_PAGE_LIST_PAGE_TUPLES_UTIL_H
#define LINKED_PAGE_LIST_PAGE_TUPLES_UTIL_H

/*
**	linked_page_list_node_util handles managing next and prev page pointers on the page of a linked_page_list
**	while the linked_page_list_page_tuples_util handles managing tuples (splits/merges) on the page of a linked_page_list
*/

//#include<stdint.h>

#include<persistent_page.h>
#include<linked_page_list_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// check if a linked_page_list page must split for an insertion of a tuple on the page
int must_split_for_insert_linked_page_list_page(const persistent_page* page1, const void* tuple_to_insert, const linked_page_list_tuple_defs* lpltd_p);

//persistent_page split_insert_bplus_tree_interior_page(persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, int split_upper_half/lower_half, int vacant_in_upper_half/vacant_page2/OR_equal_split, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// for merging page1->next_page == page2 and page2->prev_page == page1

// check if 2 linked_page_list pages can be merged
int can_merge_linked_page_list_pages(const persistent_page* page1, const persistent_page* page2, const linked_page_list_tuple_defs* lpltd_p);

// it performs merge of the 2 linked_page_list pages (page1 and page2)
// for merging page1 and page2 must be adjacent, i.e. page1->next_page == page2 and page2->prev_page == page1
// it fails with a 0 if the pages can not be merged (this may be due to their used spaces greater than the allotted size on the page1)
// lock on page1 and page2 are not released, no other page locks are acquired
// if MERGE_INTO_PAGE1 is called, then upon a successfull merge tuples of page2 are copied at the end of page1, contents of page2 are untouched
// else if MERGE_INTO_PAGE2 is called, then upon a successfull merge tuples of page1 are copied at the beginning of page2, contents of page1 are untouched
// even after a successful merge none of the pages, are removed from the linkage of linked_page_list, as explained earlier, this is the job of linked_page_list_node_util source and header files
#define MERGE_INTO_PAGE1 0
#define MERGE_INTO_PAGE2 1
int merge_linked_page_list_pages(persistent_page* page1, persistent_page* page2, int merge_into, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif