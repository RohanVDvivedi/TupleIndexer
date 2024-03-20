#ifndef LINKED_PAGE_LIST_PAGE_TUPLES_UTIL_H
#define LINKED_PAGE_LIST_PAGE_TUPLES_UTIL_H

/*
**	linked_page_list_node_util handles managing next and prev page pointers on the page of a linked_page_list
**	while the linked_page_list_page_tuples_util handles managing tuples (splits/merges) on the page of a linked_page_list
*/

//#include<stdint.h>

#include<persistent_page.h>
#include<linked_page_list_tuple_definitions.h>
//#include<opaque_page_access_methods.h>
//#include<opaque_page_modification_methods.h>

// check if a linked_page_list page must split for an insertion of a tuple on the page
int must_split_for_insert_linked_page_list_page(const persistent_page* page1, const void* tuple_to_insert, const linked_page_list_tuple_defs* lpltd_p);

//persistent_page split_insert_bplus_tree_interior_page(persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, int split_upper_half/lower_half, int vacant_in_upper_half/vacant_page2/OR_equal_split, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// check if 2 linked_page_list pages can be merged
int can_merge_linked_page_list_pages(const persistent_page* page1, const persistent_page* page2, const linked_page_list_tuple_defs* lpltd_p);

//int merge_linked_page_list_pages(persistent_page* page1, persistent_page* page2, int merge_into_page2/page1, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif