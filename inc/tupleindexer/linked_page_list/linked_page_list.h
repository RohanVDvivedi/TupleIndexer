#ifndef LINKED_PAGE_LIST_H
#define LINKED_PAGE_LIST_H

#include<linked_page_list_tuple_definitions_public.h>

#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// returns pointer to the root page of the newly created linked_page_list
uint64_t get_new_linked_page_list(const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the linked_page_list
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given linked_page_list
int destroy_linked_page_list(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the pages in the linked_page_list
// it may return an abort_error, unable to print all of the linked_page_list pages
void print_linked_page_list(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// merge two linked_page_lists, the final merged linked_page_list has root_page_id as lpl1_head_page_id
// all tuples of the lpl2 will be logically appended at the end of the contents of lpl1
int merge_linked_page_lists(uint64_t lpl1_head_page_id, uint64_t lpl2_head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#include<linked_page_list_iterator_public.h>

#endif