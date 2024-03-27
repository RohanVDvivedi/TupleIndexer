#ifndef LINKED_PAGE_LIST_H
#define LINKED_PAGE_LIST_H

// returns pointer to the root page of the newly created linked_page_list
uint64_t get_new_linked_page_list(const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the linked_page_list
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given linked_page_list
int destroy_linked_page_list(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the pages in the linked_page_list
// it may return an abort_error, unable to print all of the linked_page_list pages
void print_linked_page_list(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif