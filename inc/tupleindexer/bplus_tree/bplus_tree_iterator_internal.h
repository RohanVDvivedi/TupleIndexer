#ifndef BPLUS_TREE_ITERATOR_INTERNAL_H
#define BPLUS_TREE_ITERATOR_INTERNAL_H

#include<tupleindexer/bplus_tree/bplus_tree_iterator.h>

bplus_tree_iterator* get_new_bplus_tree_stacked_iterator(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);
bplus_tree_iterator* get_new_bplus_tree_unstacked_iterator(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// bpi_p is assumed to be garbage initialized, and is fully initialized only on success
// lps must not be NULL
// provide either a valid non empty lps or a root_page_id
// upon success all lps pages are transferred to the bpi_p
// on abort_error all locks held in the lps are released
// lps has to be deinitialized after any call to this function
int initialize_bplus_tree_stacked_iterator(bplus_tree_iterator* bpi_p, uint64_t root_page_id, locked_pages_stack* lps, const void* key_OR_record, int is_key, uint32_t key_element_count_concerned, find_position find_pos, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);
#define initialize_bplus_tree_stacked_iterator_using_key(bpi_p, root_page_id, lps, key, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)			initialize_bplus_tree_stacked_iterator(bpi_p, root_page_id, lps, key, 1, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)
#define initialize_bplus_tree_stacked_iterator_using_record(bpi_p, root_page_id, lps, record, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)		initialize_bplus_tree_stacked_iterator(bpi_p, root_page_id, lps, record, 0, key_element_count_concerned, find_pos, lock_type, bpttd_p, pam_p, pmm_p, transaction_id, abort_error)
int initialize_bplus_tree_unstacked_iterator(bplus_tree_iterator* bpi_p, uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// private function to get the current lockd leaf page
persistent_page* get_curr_leaf_page(bplus_tree_iterator* bpi_p);

#endif