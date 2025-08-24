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

int destroy_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	heap_table_iterator* hti_p = NULL;

	hti_p = get_new_heap_table_iterator(root_page_id, 0, 0, httd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// free each heap page one by one
	while(1)
	{
		uint32_t unused_space;
		uint64_t page_id = get_curr_heap_page_id_heap_table_iterator(hti_p, &unused_space);

		// we are beyond the last heap_table entry so break out
		if(page_id == httd_p->pas_p->NULL_PAGE_ID)
			break;

		// free the page_id
		free_persistent_page(pam_p, transaction_id, page_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// goto to the next entry
		next_heap_table_iterator(hti_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	delete_heap_table_iterator(hti_p, transaction_id, abort_error);
	hti_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	destroy_bplus_tree(root_page_id, &(httd_p->bpttd), pam_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return 1;

	ABORT_ERROR:;
	if(hti_p != NULL)
	{
		delete_heap_table_iterator(hti_p, transaction_id, abort_error);
		hti_p = NULL;
	}
	return 0;
}

void print_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	heap_table_iterator* hti_p = NULL;

	hti_p = get_new_heap_table_iterator(root_page_id, 0, 0, httd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// print the root page id of the heap_table
	printf("\n\nHeap_table @ root_page_id = %"PRIu64"\n\n", root_page_id);

	debug_print_lock_stack_for_heap_table_iterator(hti_p);

	// free each heap page one by one
	while(1)
	{
		uint32_t unused_space;
		persistent_page ppage = lock_and_get_curr_heap_page_heap_table_iterator(hti_p, 0, &unused_space, NULL, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// ensure it is not end of the scan
		if(is_persistent_page_NULL(&ppage, pam_p))
			break;

		// print the entry
		debug_print_lock_stack_for_heap_table_iterator(hti_p);
		printf("(unused_space = %"PRIu32", page_id = %"PRIu64")\n\n", unused_space, ppage.page_id);

		// print the heap_page
		print_heap_page(&ppage, httd_p->pas_p, httd_p->record_def);
		printf("\n\n");

		// release lock on it
		release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// goto to the next entry
		next_heap_table_iterator(hti_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	debug_print_lock_stack_for_heap_table_iterator(hti_p);

	delete_heap_table_iterator(hti_p, transaction_id, abort_error);
	hti_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	return;

	ABORT_ERROR:;
	if(hti_p != NULL)
	{
		delete_heap_table_iterator(hti_p, transaction_id, abort_error);
		hti_p = NULL;
	}
	return;
}

persistent_page find_heap_page_with_enough_unused_space_from_heap_table(uint64_t root_page_id, const uint32_t required_unused_space, uint32_t* unused_space_in_entry, const heap_table_notifier* notify_wrong_entry, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	heap_table_iterator* hti_p = NULL;
	persistent_page ppage = get_NULL_persistent_page(pam_p);

	hti_p = get_new_heap_table_iterator(root_page_id, required_unused_space, 0, httd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// free each heap page one by one
	while(1)
	{
		int is_fix_needed = 0;
		ppage = lock_and_get_curr_heap_page_heap_table_iterator(hti_p, 1, unused_space_in_entry, &is_fix_needed, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// ensure it is not end of the scan
		if(is_persistent_page_NULL(&ppage, pam_p))
			break;

		// make sure if the fix is needed, if so notify it
		if(is_fix_needed && notify_wrong_entry != NULL)
			notify_wrong_entry->notify(notify_wrong_entry->context, (*unused_space_in_entry), ppage.page_id);

		// we found the right entry, some page with just enough unused_space
		if(get_unused_space_on_heap_page(&ppage, httd_p->pas_p, httd_p->record_def) >= required_unused_space)
			break;

		// release lock on it and continue for the next entry
		release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// goto to the next entry
		next_heap_table_iterator(hti_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	delete_heap_table_iterator(hti_p, transaction_id, abort_error);
	hti_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	return ppage;

	ABORT_ERROR:;
	if(!is_persistent_page_NULL(&ppage, pam_p))
		release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
	if(hti_p != NULL)
	{
		delete_heap_table_iterator(hti_p, transaction_id, abort_error);
		hti_p = NULL;
	}
	return get_NULL_persistent_page(pam_p);
}