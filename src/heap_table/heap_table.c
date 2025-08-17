#include<tupleindexer/heap_table/heap_table.h>

#include<tupleindexer/heap_table/heap_table_tuple_definitions.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>
#include<tupleindexer/heap_page/heap_page.h>

uint64_t get_new_heap_table(const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	return get_new_bplus_tree(&(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
}

typedef struct fix_remove_inspect_context fix_remove_inspect_context;
struct fix_remove_inspect_context
{
	persistent_page* ppage;

	const heap_table_tuple_defs* httd_p;

	const page_access_methods* pam_p;
};

static int fix_remove_inspect(const void* context, const tuple_def* record_def, const void* old_record, void** new_record, void (*cancel_update_callback)(void* cancel_update_callback_context, const void* transaction_id, int* abort_error), void* cancel_update_callback_context, const void* transaction_id, int* abort_error)
{
	// we are removing a record for sure, so set this to NULL
	(*new_record) = NULL;

	persistent_page* ppage = ((fix_remove_inspect_context*)context)->ppage;
	const heap_table_tuple_defs* httd_p = ((fix_remove_inspect_context*)context)->httd_p;
	const page_access_methods* pam_p = ((fix_remove_inspect_context*)context)->pam_p;

	// find the page_id and the unused_space from the entry found
	uint32_t unused_space;
	uint64_t page_id = decompose_heap_table_entry_tuple(httd_p, old_record, &unused_space);

	// lock the prescribed page
	(*ppage) = acquire_persistent_page_with_lock(pam_p, transaction_id, page_id, READ_LOCK, abort_error);
	if(*abort_error) // on abort nothing to release lock on
		return 0;

	// fetch the actual unused_space on the heap_page in context
	uint32_t actual_unused_space = get_unused_space_on_heap_page(ppage, httd_p->pas_p, httd_p->record_def);

	// if the unused_space on the heap_table entry is same as that on the page, then no need to perform the remove
	if(unused_space == actual_unused_space)
	{
		// this is the last statement, after a failure, so no need to check abort_error
		release_lock_on_persistent_page(pam_p, transaction_id, ppage, NONE_OPTION, abort_error);
		return 0;
	}

	// else keep the page locked and ask to remove the entry
	return 1;
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
	int removed = inspected_update_in_bplus_tree(root_page_id, entry_tuple, &((update_inspector){.context = &(fix_remove_inspect_context){&ppage, httd_p, pam_p}, .update_inspect = fix_remove_inspect}), &(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	if(!removed) // possibly a stale entry OR a correct unused_space entry, so nothing needs to be done
	{
		// release lock on the heap page it was acquired
		if(!is_persistent_page_NULL(&ppage, pam_p))
		{
			// this is the last statement, after a failure, so no need to check abort_error
			release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
		}
		return 0;
	}

	// now heap page is surely locked

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
		uint32_t unused_space = get_unused_space_on_heap_page(&ppage, httd_p->pas_p, httd_p->record_def);

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

	// else create and insert the entry (unused_space, page_id) in the heap_table

	uint32_t unused_space = get_unused_space_on_heap_page(ppage, httd_p->pas_p, httd_p->record_def);

	char entry_tuple[HEAP_TABLE_ENTRY_TUPLE_MAX_SIZE];
	build_heap_table_entry_tuple(httd_p, entry_tuple, unused_space, ppage->page_id);

	int inserted = insert_in_bplus_tree(root_page_id, entry_tuple, &(httd_p->bpttd), pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	return inserted;
}

int destroy_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

void print_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);