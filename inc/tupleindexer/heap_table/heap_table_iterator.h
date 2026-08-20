#ifndef HEAP_TABLE_ITERATOR_H
#define HEAP_TABLE_ITERATOR_H

#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<tupleindexer/heap_table/heap_table_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

/*
	This is a read-only, leaf-only, forward-only bplus_tree iterator over the heap_table
	But you can request a lock on the heap_page in a read or even write locked mode
	If you have tuples you want to insert during this scan, you possibly need to close this scan and open a new scan, find a right best-fit heap_page and insert into it
	relatch and start scanning from the previous heap_page-s unused_space and page_id

	if your architecture and logic permits and there is enough space on the heap_page, you can modify it right here in this scan

	if you want to prevent heap_table restructuring during this whole time you probably need a shared/exclusive lock to prevent this (by letting fix_* and track_* functions accesses in exclusive mode only)
*/

typedef struct heap_table_iterator heap_table_iterator;
struct heap_table_iterator
{
	// actual iterator over the heap_table's bplus_tree
	// allows only read-only, leaf-only and forward only accesses
	bplus_tree_iterator* bpi_p;

	const heap_table_tuple_defs* httd_p;

	const page_access_methods* pam_p;
};

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
// entry_needs_fixing variable may be passed NULL, if passed and gets set, then you may later (after deleting this iterator) go ahead with calling fix_unused_space_in_heap_table() on this entry, this flag suggests if the entry has a wrong unused_space on it
persistent_page lock_and_get_curr_heap_page_heap_table_iterator(const heap_table_iterator* hti_p, int write_locked, uint32_t* unused_space, int* entry_needs_fixing, const void* transaction_id, int* abort_error);

int next_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error);

void delete_heap_table_iterator(heap_table_iterator* hti_p, const void* transaction_id, int* abort_error);

void debug_print_lock_stack_for_heap_table_iterator(heap_table_iterator* hti_p);

#endif