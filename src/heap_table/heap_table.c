#include<tupleindexer/heap_table/heap_table.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>

uint64_t get_new_heap_table(const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return get_new_bplus_tree(&(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
}