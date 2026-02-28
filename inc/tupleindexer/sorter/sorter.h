#ifndef SORTER_H
#define SORTER_H

#include<tupleindexer/sorter/sorter_tuple_definitions_public.h>

#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

#include<tupleindexer/linked_page_list/linked_page_list_iterator_public.h>

/*
	Note: The sorter is a single threaded structure.
	Only the merge_sorted_runs_in_sorter is concurrent and thread safe
	You may call this function (*only this one), any time concurrently with any other function except with get_new_sorter() and destroy_sorter()
*/

// this is how big the queue for storing the sorted runs using the page_table can become
#define SORTED_RUNS_CAPACITY UINT64_MAX

typedef struct sorter_locker sorter_locker;
struct sorter_locker
{
	void* sorter_lock;
	void (*lock)(void* sorter_lock);
	void (*unlock)(void* sorter_lock);
};

typedef struct sorter_handle sorter_handle;
struct sorter_handle
{
	// page_table storing all runs
	uint64_t sorted_runs_root_page_id;

	// below three attributes allows us to index into the page_table pointed to by the sorted_runs_root_page_id
	// allowing us to use this page_table as a curcular queue
	uint64_t sorted_runs_first_index;
	uint64_t sorted_runs_count;
	uint64_t sorted_runs_slots_reserved_count;

	// uses a user provided locker interface to protect the sorted_runs queue
	sorter_locker slocker;

	// iterator to the last partial run, and its head_page_id
	// this run does not exist in the runs_root_page_id
	// it is to be inserted after sorting, after it gets full
	uint64_t unsorted_partial_run_head_page_id;
	linked_page_list_iterator* unsorted_partial_run;

	// boiler plate

	const sorter_tuple_defs* std_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
};

// this function fails silently if any of std_p, pam_p OR pmm_p are NULL
// pass an all empty (sorter_locker){}, if you do not want to use it
sorter_handle get_new_sorter(sorter_locker slocker, const sorter_tuple_defs* std_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// inserts 1 record into the sorter
// calling this function with a NULL record, pushes the last partially unsorted run into the sorter
// on an abort error, all iterators are closed, no need to call destroy_sorter
int insert_in_sorter(sorter_handle* sh_p, const void* record, const void* transaction_id, int* abort_error);

// picks atmost N_way number of sorted runs and tries to merge them into 1
// on success, returns 1, and otherwise 0 if there is only 1 run in the sorted_runs
// ideally call this function in loop, in several different threads, until all your inserts are completed
// on an abort error, all iterators are closed, no need to call destroy_sorter
int merge_N_runs_in_sorter(sorter_handle* sh_p, uint32_t N_way, const void* transaction_id, int* abort_error);

/*
	sorter is like a datastructure, you destroy it only if all prior calls succeeded without an abort_error
*/

// destroys the sorter, destroying every run except for the first run, if the sorted_data is not NULL
// it will also destroy the unsorted_partial_run, and close all iterators
// you need to call this function only on success, on abort error, you do not need to call this function
int destroy_sorter(sorter_handle* sh_p, uint64_t* sorted_data, const void* transaction_id, int* abort_error);

#endif