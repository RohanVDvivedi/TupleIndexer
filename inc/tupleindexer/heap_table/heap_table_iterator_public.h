#ifndef HEAP_TABLE_ITERATOR_PUBLIC_H
#define HEAP_TABLE_ITERATOR_PUBLIC_H

typedef struct heap_table_iterator heap_table_iterator;

#include<tupleindexer/utils/persistent_page_functions.h>

// creates a new heap_table_iterator starting with or after the provided entry for (unused_space, page_id), if you only looking for a best-fit page to insert into then you may pass page_id = 0
// on abort_error, NULL is returned
heap_table_iterator* get_new_heap_table_iterator(uint64_t root_page_id, uint32_t unused_space, uint64_t page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// returns NULL on an abort error
// on an abort_error, hti_p will still hold its locks
heap_table_iterator* clone_heap_table_iterator(const heap_table_iterator* hti_p, const void* transaction_id, int* abort_error);

// returns NULL_PAGE_ID if you are at the end of the scan OR if the heap_table is empty
uint64_t get_curr_heap_page_id_heap_table_iterator(const heap_table_iterator* hti_p, uint32_t* unused_space);

// returns get_NULL_persistent_page() if you are at the end of the scan OR if the heap_table is empty
persistent_page lock_and_get_curr_heap_page_heap_table_iterator(const heap_table_iterator* hti_p, int write_locked, uint32_t* unused_space, const void* transaction_id, int* abort_error);

int next_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error);

void delete_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error);

#endif