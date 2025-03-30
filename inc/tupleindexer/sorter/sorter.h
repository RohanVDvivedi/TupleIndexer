#ifndef SORTER_H
#define SORTER_H

#include<sorter_tuple_definitions_public.h>

#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

#include<linked_page_list_iterator_public.h>

/*
	Note: The sorter is a single threaded structure, i.e. is thread unsafe by design and only 1 thread must be accessing it at a time, hence an external lock is necesary to use it, for concurrent accesses.
	To gain performance on sorting you may tweak to a higher value of N_way, but this will happen at the cost of more pages being locked at once. (in the active runs of the iterators)
*/

typedef struct sorter_handle sorter_handle;
struct sorter_handle
{
	// page_table storing all runs
	uint64_t sorted_runs_root_page_id;

	// number of total runs in the page_table at root_page_id
	uint64_t sorted_runs_count;

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
sorter_handle get_new_sorter(const sorter_tuple_defs* std_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// inserts 1 record into the sorter
// on an abort error, all iterators are closed, no need to call destroy_sorter
int insert_in_sorter(sorter_handle* sh_p, const void* record, const void* transaction_id, int* abort_error);

// performs external_sort_merge on the runs present in the sorter
// it attempts to merge N runs at a time into 1 run
// it will do this until there is more than 1 runs in the sorted_runs_root_page_id
// here N_way must be >= 2, else the default value is set to 2
// this function also has the optimization to inject new runs into the sorter, if the last tuple inserted is lesser than or equal to the first tuple of the new run
// on an abort error, all iterators are closed, no need to call destroy_sorter
int external_sort_merge_sorter(sorter_handle* sh_p, uint64_t N_way, const void* transaction_id, int* abort_error);

// destroys the sorter, destroying every run except for the first run, if the sorted_data is not NULL
// it will also destroy the unsorted_partial_run, and close all iterators
int destroy_sorter(sorter_handle* sh_p, uint64_t* sorted_data, const void* transaction_id, int* abort_error);

#endif