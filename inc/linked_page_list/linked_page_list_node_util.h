#ifndef LINKED_PAGE_LIST_NODE_UTIL_H
#define LINKED_PAGE_LIST_NODE_UTIL_H

#include<persistent_page.h>
#include<linked_page_list_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// initialize linked_page_list page, with next_page and prev_page pointing to NULL_PAGE_IDs
int init_linked_page_list_page(persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// insert a page in between ppage_xist1 and ppage_xist2, given that ppage_xist1->next = ppage_xist2 (else it fails)
int insert_page_in_between_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2,  persistent_page* ppage_to_ins, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// remove a ppage_xist2 from in between ppage_xist1 and ppage_xist3, given that ppage_xist1->next = ppage_xist2 && ppage_xist2->next = ppage_xist3 (else it fails)
int remove_page_in_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2, persistent_page* ppage_xist3, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif