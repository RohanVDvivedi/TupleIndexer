#include<linked_page_list_node_util.h>

int init_linked_page_list_page(persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int is_next_of_in_linked_page_list(const persistent_page* ppage, const persistent_page* ppage_test_next, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int is_prev_of_in_linked_page_list(const persistent_page* ppage, const persistent_page* ppage_test_prev, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int is_singular_head_linked_page_list(const persistent_page* ppage_head, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int is_dual_node_linked_page_list(const persistent_page* ppage_head, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

persistent_page lock_and_get_next_ppage_in_linked_page_list(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

persistent_page lock_and_get_prev_ppage_in_linked_page_list(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int insert_page_in_between_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2,  persistent_page* ppage_to_ins, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int remove_page_in_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2, persistent_page* ppage_xist3, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);
