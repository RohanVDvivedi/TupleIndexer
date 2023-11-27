#include<page_table.h>

uint64_t get_new_page_table(const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int destroy_page_table(uint64_t root_page_id, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

void print_page_table(uint64_t root_page_id, int only_leaf_pages, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);