#include<tupleindexer/heap_table/heap_table.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>
#include<tupleindexer/heap_page/heap_page.h>

uint64_t get_new_heap_table(const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return get_new_bplus_tree(&(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
}

int fix_unused_space_in_heap_table(uint64_t root_page_id, uint32_t unused_space, uint64_t page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// if the page_id is NULL fail immediately
	if(page_id == httd_p->pas_p->NULL_PAGE_ID)
		return 0;

	persistent_page ppage = get_NULL_persistent_page(pam_p);

	char entry_tuple[HEAP_TABLE_ENTRY_TUPLE_MAX_SIZE];
	build_heap_table_entry_tuple(httd_p, entry_tuple, unused_space, page_id);

	// if this functions succeeds we will have the update_inspector locked the concerned heap_page in the ppage for us
	int removed = inspected_update_in_bplus_tree(root_page_id, entry_tuple, const update_inspector* ui_p, &(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	if(!removed) // possibly a stale entry, nothing needs to be done
		return 0;

	// now ppage is surely locked with the right page

	// if this page is empty, free it
	if(is_heap_page_empty(&ppage, httd_p->pas_p, httd_p->record_def))
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &ppage, FREE_PAGE, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}
	else // else first reinsert the page, and then release lock on it
	{
		// fetch unused_space on this page
		uint32_t unused_space = get_unused_space_on_heap_page(ppage, httd_p->pas_p, httd_p->record_def);

		// build the entry for this tuple, that we need to insert
		build_heap_table_entry_tuple(httd_p, entry_tuple, unused_space, page_id);

		// insert a new entry for it
		insert_in_bplus_tree(root_page_id, entry_tuple, &(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// release lock on it
		release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// everything went as planned
	return 1;

	ABORT_ERROR:
	if(!is_persistent_page_NULL(&ppage, pam_p))
		release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
	return 0;
}

int track_unused_space_in_heap_table(uint64_t root_page_id, const persistent_page* ppage, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// if the ppage is NULL fail immediately
	if(is_persistent_page_NULL(ppage, pam_p))
		return 0;

	uint32_t unused_space = get_unused_space_on_heap_page(ppage, httd_p->pas_p, httd_p->record_def);

	char entry_tuple[HEAP_TABLE_ENTRY_TUPLE_MAX_SIZE];
	build_heap_table_entry_tuple(httd_p, entry_tuple, unused_space, ppage->page_id);

	return insert_in_bplus_tree(root_page_id, entry_tuple, &(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
}

int destroy_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

void print_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);