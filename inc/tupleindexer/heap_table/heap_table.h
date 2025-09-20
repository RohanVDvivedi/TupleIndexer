#ifndef HEAP_TABLE_H
#define HEAP_TABLE_H

/*
	heap_table is a derivative data structure built entirely on top of bplus_tree
	it is only usefull to perform free space management and tracking all pages in a heap_table (concept similar to postgresql)
	heap_table only manages heap_page-s by storing their (unsed_space, page_id) entry in a bplus_tree
	to find the best-fit to perform an insert into an existing heap_page
	ideally the unused_space in the heap_table is a hint and not a source of truth, i.e. it has to be eventually consistent, else you encounter redundant write amplification

	as expected the (page_id, tuple_index) in heap_page-s of heap_table are meant to be fixed fixed for the life span of the tuple, thus allowing us to store (page_id, tuple_index) in external indexes (b+tree or hash_table)
	and this (page_id, tuple_index) can be treated as a pointer to the tuple
	also you do not need to lock an scan the heap_table to access a heap_page, instead you just need to directly lock the heap_page and access the tuple, or possibly modify it
	but please ensure to eventually fix the unused_space entry in the heap_table AFTER THE HEAP_PAGE LOCK HAS BEEN RELEASED synchronously or asynchronously

	additionally you can also use the heap_table_iterator to iterate over all the heap_page-s and scan all the tuples, but please be sure that fix_* and track_* functions may reorder heap_pages
	requiring you to take a lock in shared mode for a complete scan and in exclusive mode to fix_* unused_space entries

	it is basically a big-bad-wrapper over the bplus_tree to manage unused space and best-fit allocation in a heap_table, (like the FSM used in )
*/

#include<tupleindexer/heap_table/heap_table_tuple_definitions_public.h>

#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

#include<tupleindexer/utils/persistent_page_functions.h>

// returns pointer to the root page of the newly created heap_table
uint64_t get_new_heap_table(const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// fix and correct an entry (unused_space, page_id) in the heap_table, by removing that entry itself and reinserting with the correct unused_space
// if the page in context is identified to be empty, then a removal is performed instead of a reinsert
// this function is a NO-OP if page_id == NULL_PAGE_ID
int fix_unused_space_in_heap_table(uint64_t root_page_id, uint32_t unused_space, uint64_t page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// to be called after you allocate and insert all tuples in a heap_page and then you want it to be tracked by this heap_table
// the ppage is only read by this function to decipher its unused_space, you have to release lock on it, in any possible case (cases being abort_error or not)
// this function is a NO-OP if ppage is a NULL_persistent_page
int track_unused_space_in_heap_table(uint64_t root_page_id, const persistent_page* ppage, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the heap_table and the tracked heap_page-s
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given heap_table
int destroy_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the heap_page-s in the heap_table in order of their unused_space as in the heap_table
// it may return an abort_error, unable to print all of the heap_table pages
void print_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// a read utility to get the current maximum level this heap_table hosts, this can be used to approximate the number of buffer pages required
uint32_t get_root_level_heap_table(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// interface to allow external world about a wrong heap_table entry, i.e. entry whcih needs to be fixed
// you possibly should insert such entries into a hashmap<page_id, unused_space> to fix_*() them later on
typedef struct heap_table_notifier heap_table_notifier;
struct heap_table_notifier
{
	void* context;
	void (*notify)(void* context, uint32_t unused_space, uint64_t page_id);
};

// write lock and get a heap_page from the heap_table, that has unused_space >= required_unused_space
// once a heap_page is identified, fill it up to the brim inserting all the possible tuple you could, and then mark it to be fixed
persistent_page find_heap_page_with_enough_unused_space_from_heap_table(uint64_t root_page_id, const uint32_t required_unused_space, uint32_t* unused_space_in_entry, const heap_table_notifier* notify_wrong_entry, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#include<tupleindexer/heap_table/heap_table_iterator_public.h>

#endif