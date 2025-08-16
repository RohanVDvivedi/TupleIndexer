#include<tupleindexer/heap_table/heap_table_iterator.h>

#include<stdlib.h>

heap_table_iterator* get_new_heap_table_iterator(uint64_t root_page_id, uint32_t unused_space, uint64_t page_id, const void* key, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
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

// returns NULL_PAGE_ID if you are at the end of the scan OR if the heap_table is empty
uint64_t get_curr_heap_page_id_heap_table_iterator(const heap_table_iterator* hti_p, uint32_t* unused_space);

// returns get_NULL_persistent_page() if you are at the end of the scan OR if the heap_table is empty
persistent_page lock_and_get_curr_heap_page_heap_table_iterator(const heap_table_iterator* hti_p, int write_locked);

int next_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	return next_bplus_tree_iterator(hti_p->bpi_p, transaction_id, abort_error);
}

void delete_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	delete_bplus_tree_iterator(hti_p->bpi_p, transaction_id, abort_error);
	free(hti_p);
}