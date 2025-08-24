#include<tupleindexer/heap_table/heap_table_iterator.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<stdlib.h>

heap_table_iterator* get_new_heap_table_iterator(uint64_t root_page_id, uint32_t unused_space, uint64_t page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	heap_table_iterator* hti_p = malloc(sizeof(heap_table_iterator));
	if(hti_p == NULL)
		exit(-1);

	hti_p->httd_p = httd_p;
	hti_p->pam_p = pam_p;

	char entry_tuple[HEAP_TABLE_ENTRY_TUPLE_MAX_SIZE];
	build_heap_table_entry_tuple(httd_p, entry_tuple, unused_space, page_id);

	// start an unstacked read locked iterator
	hti_p->bpi_p = find_in_bplus_tree(root_page_id, entry_tuple, 2, GREATER_THAN_EQUALS, 0, READ_LOCK, &(httd_p->bpttd), pam_p, NULL, transaction_id, abort_error);
	if(*abort_error)
	{
		free(hti_p);
		return NULL;
	}

	return hti_p;
}

heap_table_iterator* clone_heap_table_iterator(const heap_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	heap_table_iterator* clone_p = malloc(sizeof(heap_table_iterator));
	if(clone_p == NULL)
		exit(-1);

	clone_p->httd_p = hti_p->httd_p;
	clone_p->pam_p = hti_p->pam_p;

	clone_p->bpi_p = clone_bplus_tree_iterator(hti_p->bpi_p, transaction_id, abort_error);
	if(*abort_error)
	{
		free(clone_p);
		return NULL;
	}

	return clone_p;
}

uint64_t get_curr_heap_page_id_heap_table_iterator(const heap_table_iterator* hti_p, uint32_t* unused_space)
{
	if(is_empty_bplus_tree(hti_p->bpi_p))
		return hti_p->httd_p->pas_p->NULL_PAGE_ID;

	if(is_beyond_max_tuple_bplus_tree_iterator(hti_p->bpi_p))
		return hti_p->httd_p->pas_p->NULL_PAGE_ID;

	return decompose_heap_table_entry_tuple(hti_p->httd_p, get_tuple_bplus_tree_iterator(hti_p->bpi_p), unused_space);
}

persistent_page lock_and_get_curr_heap_page_heap_table_iterator(const heap_table_iterator* hti_p, int write_locked, uint32_t* unused_space, int* entry_needs_fixing, const void* transaction_id, int* abort_error)
{
	uint64_t page_id = get_curr_heap_page_id_heap_table_iterator(hti_p, unused_space);

	if(page_id == hti_p->httd_p->pas_p->NULL_PAGE_ID)
		return get_NULL_persistent_page(hti_p->pam_p);

	persistent_page ppage = acquire_persistent_page_with_lock(hti_p->pam_p, transaction_id, page_id, ((write_locked) ? WRITE_LOCK : READ_LOCK), abort_error);
	if(*abort_error)
		return get_NULL_persistent_page(hti_p->pam_p);

	// if the entry_needs_fixing is passed then compute it
	// fixing is necessary only if the heap_page is empty OR actual_unused_space is not equal to the unused_space on it's entry
	if(entry_needs_fixing != NULL)
		(*entry_needs_fixing) = is_heap_page_empty(&ppage, hti_p->httd_p->pas_p, hti_p->httd_p->record_def)
							|| ((*unused_space) != get_unused_space_on_heap_page(&ppage, hti_p->httd_p->pas_p, hti_p->httd_p->record_def));

	return ppage;
}

int next_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	return next_bplus_tree_iterator(hti_p->bpi_p, transaction_id, abort_error);
}

void delete_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	delete_bplus_tree_iterator(hti_p->bpi_p, transaction_id, abort_error);
	free(hti_p);
}

void debug_print_lock_stack_for_heap_table_iterator(heap_table_iterator* hti_p)
{
	debug_print_lock_stack_for_bplus_tree_iterator(hti_p->bpi_p);
}