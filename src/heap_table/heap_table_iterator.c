#include<tupleindexer/heap_table/heap_table_iterator.h>

heap_table_iterator* get_new_heap_table_iterator(uint64_t root_page_id, uint32_t unused_space, uint64_t page_id, const void* key, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

heap_table_iterator* clone_heap_table_iterator(const heap_table_iterator* hti_p, const void* transaction_id, int* abort_error);

// returns NULL_PAGE_ID if you are at the end of the scan OR if the heap_table is empty
uint64_t get_curr_heap_page_id_heap_table_iterator(const heap_table_iterator* hti_p, uint32_t* unused_space);

// returns get_NULL_persistent_page() if you are at the end of the scan OR if the heap_table is empty
persistent_page lock_and_get_curr_heap_page_heap_table_iterator(const heap_table_iterator* hti_p, int write_locked);

int next_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error);

void delete_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error)
{
	delete_bplus_tree_iterator(hti_p->bpi_p);
	free(hti_p);
}