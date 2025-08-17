#include<tupleindexer/heap_table/heap_table.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>
#include<tupleindexer/heap_page/heap_page.h>

uint64_t get_new_heap_table(const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return get_new_bplus_tree(&(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
}

void fix_unused_space_in_heap_table(uint64_t root_page_id, uint32_t unused_space, uint64_t page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int track_unused_space_in_heap_table(uint64_t root_page_id, const persistent_page* ppage, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(is_persistent_page_NULL(ppage, pam_p))
		return 0;

	uint32_t unused_space = get_unused_space_on_heap_page(ppage, httd_p->pas_p, httd_p->record_def);

	char entry_tuple[HEAP_TABLE_ENTRY_TUPLE_MAX_SIZE];
	build_heap_table_entry_tuple(httd_p, entry_tuple, unused_space, ppage->page_id);

	return insert_in_bplus_tree(root_page_id, entry_tuple, &(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
}

int destroy_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

void print_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);