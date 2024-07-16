#include<page_table.h>

#include<array_table.h>
#include<persistent_page_functions.h>
#include<locked_pages_stack.h>

#include<stdlib.h>

uint64_t get_new_page_table(const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return get_new_array_table(&(pttd_p->attd), pam_p, pmm_p, transaction_id, abort_error);
}

int destroy_page_table(uint64_t root_page_id, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	return destroy_array_table(root_page_id, &(pttd_p->attd), pam_p, transaction_id, abort_error);
}

void print_page_table(uint64_t root_page_id, int only_leaf_pages, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	return print_page_table(root_page_id, only_leaf_pages, &(pttd_p->attd), pam_p, transaction_id, abort_error);
}