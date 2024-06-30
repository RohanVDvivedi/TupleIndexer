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
	sh.unsorted_partial_run = NULL;

	// create a page_table for holding the sorted runs
	// failure here is reported by the abort_error if any, else it is a success
	sh.sorted_runs_root_page_id = get_new_page_table(&(std_p->pttd), pam_p, pmm_p, transaction_id, abort_error);

	return sh;
}

// if there exists a unsorted partial run, then it is sorter and put at the end of the sorted runs
// ensure that the unsorted_partial_run exists, and it is not empty and as a HEAD_ONLY_LINKED_PAGE_LIST
// on an ABORT_ERROR, all iterators including the ones in the sorter_handle are closed
static int consume_unsorted_partial_run_from_sorter(sorter_handle* sh_p, const void* transaction_id, int* abort_error)
{
	// handle that will be maintained while accessing the runs page_table
	page_table_range_locker* ptrl_p = NULL;

	// fail if there is no unsorted_partial_run
	if(sh_p->unsorted_partial_run == NULL)
		return 0;

	// ensure that the unsorted_partial is an ONLY_HEAD_LINKED_PAGE_LIST and is not empty
	if(is_empty_linked_page_list(sh_p->unsorted_partial_run) || HEAD_ONLY_LINKED_PAGE_LIST != get_state_for_linked_page_list(sh_p->unsorted_partial_run))
		return 0;

	// sort the unsorted_partial_run
	sort_all_tuples_on_curr_page_in_linked_page_list_iterator(sh_p->unsorted_partial_run, sh_p->std_p->key_element_ids, sh_p->std_p->key_compare_direction, sh_p->std_p->key_element_count, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// close the iterator on unsorted_partial_run
	delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	sh_p->unsorted_partial_run = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	// copy the unsorted_partial_run_head_page_id into a local variable, and then reset it
	uint64_t new_sorted_run = sh_p->unsorted_partial_run_head_page_id;
	sh_p->unsorted_partial_run_head_page_id = sh_p->std_p->lpltd.pas_p->NULL_PAGE_ID;

	// ptrl[sorted_runs_count++] = unsorted_partial_run_head_page_id
	{
		ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		set_in_page_table(ptrl_p, sh_p->sorted_runs_count++, new_sorted_run, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
		ptrl_p = NULL;
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

int insert_in_sorter(sorter_handle* sh_p, const void* record, const void* transaction_id, int* abort_error)
{
	// check that the record could be inserted to the runs (linked_page_list)
	if(!check_if_record_can_be_inserted_for_sorter_tuple_definitions(sh_p->std_p, record))
		return 0;

	// if the unsorted_partial_run is full, then consume all the tuples of the unsorted_partial_run
	// sh_p->unsorted_partial_run may be equal to NULL, if the sorter_handle has just been initialized or sorted using external_sort_merge_sorter
	if(sh_p->unsorted_partial_run != NULL && !can_insert_without_split_at_linked_page_list_iterator(sh_p->unsorted_partial_run, record))
	{
		consume_unsorted_partial_run_from_sorter(sh_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	// if now the iterator is NULL, then create a new unsorted_partial_run
	if(sh_p->unsorted_partial_run == NULL)
	{
		sh_p->unsorted_partial_run_head_page_id = get_new_linked_page_list(&(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		sh_p->unsorted_partial_run = get_new_linked_page_list_iterator(sh_p->unsorted_partial_run_head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// perform the actual insert, at this point the unsorted_partial_run is neither NULL, nor FULL
	// we always insert after the tail and then go to the new tail
	{
		insert_at_linked_page_list_iterator(sh_p->unsorted_partial_run, record, INSERT_AFTER_LINKED_PAGE_LIST_ITERATOR, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		next_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	return 1;

	// all you need to do here is clean up all the open iterators
	ABORT_ERROR:;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	return 0;
}

// perform singular pass over the sorted runs in the sh_p->sorted_runs_root_page_id, sorting N active runs (N or more runs in a special case) into 1
// external_sort_merge_sorter uses this function until the sorter only consists of lesser than or equal to 1 run
// on an ABORT_ERROR, all iterators including the ones in the sorter_handle are closed
static int merge_sorted_runs_in_sorter(sorter_handle* sh_p, uint64_t N_way, const void* transaction_id, int* abort_error)
{
	if(sh_p->sorted_runs_count <= 1)
		return 0;

	// TODO
}

int external_sort_merge_sorter(sorter_handle* sh_p, uint64_t N_way, const void* transaction_id, int* abort_error)
{
	// consume unsorted runs if any
	if(sh_p->unsorted_partial_run != NULL)
	{
		consume_unsorted_partial_run_from_sorter(sh_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	while(sh_p->sorted_runs_count > 1)
	{
		merge_sorted_runs_in_sorter(sh_p, N_way, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	return 1;

	// since all internal functions handle abort internally, we do not need ABORT_ERROR label here, if we ever needed it, it would look like the commented code below
	/*
	// all you need to do here is clean up all the open iterators
	ABORT_ERROR:;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	return 0;
	*/
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