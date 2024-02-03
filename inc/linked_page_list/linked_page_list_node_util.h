#ifndef LINKED_PAGE_LIST_NODE_UTIL_H
#define LINKED_PAGE_LIST_NODE_UTIL_H

#include<persistent_page.h>
#include<linked_page_list_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// initialize linked_page_list page, with next_page and prev_page pointing to NULL_PAGE_IDs
int init_linked_page_list_page(persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// test if the ppage_test is next/prev of the ppage, i.e. test if ppage->next/prev == ppage_test_*
// only page_id of page_test_* is used
int is_next_of_in_linked_page_list(const persistent_page* ppage, const persistent_page* ppage_test_next, const linked_page_list_tuple_defs* lpltd_p);
int is_prev_of_in_linked_page_list(const persistent_page* ppage, const persistent_page* ppage_test_prev, const linked_page_list_tuple_defs* lpltd_p);

// test if ppage_head->next = ppage_head and ppage_head->prev = ppage_head
int is_singular_head_linked_page_list(const persistent_page* ppage_head, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// true, if not is_singular_head_linked_page_list AND ppage_head->next == ppage_head->prev
int is_dual_node_linked_page_list(const persistent_page* ppage_head, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// lock and get next or previous of ppage, you need to ensure that the next or previous of the ppage is not already locked by you
// returns NULL_PERSISTENT_PAGE, if the linked_page_list is is_singular_head_linked_page_list
persistent_page lock_and_get_next_ppage_in_linked_page_list(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
persistent_page lock_and_get_prev_ppage_in_linked_page_list(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// with the below functions you can use, even singular_head OR dual_node linked_page_lists

// insert a page in between ppage_xist1 and ppage_xist2, given that ppage_xist1->next = ppage_xist2 (else it fails)
int insert_page_in_between_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2,  persistent_page* ppage_to_ins, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// remove a ppage_xist2 from in between ppage_xist1 and ppage_xist3, given that ppage_xist1->next = ppage_xist2 && ppage_xist2->next = ppage_xist3 (else it fails)
int remove_page_in_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2, persistent_page* ppage_xist3, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif