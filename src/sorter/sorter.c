#include<sorter.h>

#include<page_table.h>
#include<linked_page_list.h>

sorter_handle get_new_sorter(const sorter_tuple_defs* std_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	sorter_handle sh = {};

	if(std_p == NULL || pam_p == NULL || pmm_p == NULL)
		return sh;

	sh.std_p = std_p;
	sh.pam_p = pam_p;
	sh.pmm_p = pmm_p;
	sh.sorted_runs_count = 0;
	sh.unsorted_partial_run_head_page_id = std_p->pttd.pas_p->NULL_PAGE_ID;
	sh.unsorted_partial_run = NULL;;

	// create a page_table for holding the sorted runs
	// failure here is reported by the abort_error if any, else it is a success
	sh.sorted_runs_root_page_id = get_new_page_table(&(std_p->pttd), pam_p, pmm_p, transaction_id, abort_error);

	return sh;
}

// if there exists a partial unsorted run, then it is sorter and put at the end of the sorted runs
static void consume_partial_unsorted_run_from_sorter(sorter_handle* sh_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int insert_in_sorter(sorter_handle* sh_p, const void* record, const void* transaction_id, int* abort_error)
{
	// TODO
}

int external_sort_merge_sorter(sorter_handle* sh_p, uint64_t N_way, const void* transaction_id, int* abort_error)
{
	// TODO
}

int destroy_sorter(sorter_handle* sh_p, uint64_t* sorted_data, const void* transaction_id, int* abort_error)
{
	// handle that will be maintained while accessing the runs page_table
	page_table_range_locker* ptrl_p = NULL;

	// if partial unsorted run is open destroy it
	if(sh_p->unsorted_partial_run != NULL)
	{
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
		sh_p->unsorted_partial_run = NULL;
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// destroy the linked_page_list at the unsorted_partial_run_head_page_id
	if(sh_p->unsorted_partial_run_head_page_id != sh_p->std_p->pttd.pas_p->NULL_PAGE_ID)
	{
		destroy_linked_page_list(sh_p->unsorted_partial_run_head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// now we are free from all the unsorted runs

	if(sorted_data != NULL)
	{
		ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// fetch the 0th entry and write it to (*sorted data)
		(*sorted_data) = get_from_page_table(ptrl_p, 0, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// then set the 0th entry to NULL_PAGE_ID
		set_in_page_table(ptrl_p, 0, sh_p->std_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
		ptrl_p = NULL;
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// destroy the runs page_table, and the contained runs must also be destroyed
	{
		ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, NULL, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// iterate over all the buckets and destroy them, without altering the page_table
		uint64_t bucket_id = 0;
		uint64_t bucket_head_page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, GREATER_THAN_EQUALS, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		while(bucket_head_page_id != sh_p->std_p->pttd.pas_p->NULL_PAGE_ID)
		{
			// destroy the linked_page_list (the sorted run) at the bucket_head_page_id
			destroy_linked_page_list(bucket_head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			bucket_head_page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, GREATER_THAN, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
		ptrl_p = NULL;
		if(*abort_error)
			goto ABORT_ERROR;

		// now you may destroy the page_table
		destroy_page_table(sh_p->sorted_runs_root_page_id, &(sh_p->std_p->pttd), sh_p->pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	return 1;

	// all you need to do here is clean up all the open iterators
	ABORT_ERROR:;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	if(ptrl_p != NULL)
		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	return 0;
}