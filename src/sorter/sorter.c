#include<sorter.h>

#include<page_table.h>
#include<linked_page_list.h>

#include<stdlib.h>

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
	// this check will fail insertion even if record == NULL
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

// below is the utility required to work with merge_sorted_runs_in_sorter

typedef struct active_sorted_run active_sorted_run;
struct active_sorted_run
{
	uint64_t head_page_id;
	linked_page_list_iterator* run_iterator;
};

#include<value_arraylist.h>

data_definitions_value_arraylist(active_sorted_run_heap, active_sorted_run)
declarations_value_arraylist(active_sorted_run_heap, active_sorted_run)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(active_sorted_run_heap, active_sorted_run)

static int compare_sorted_runs(const void* sh_vp, const void* asr_vp1, const void* asr_vp2)
{
	const sorter_handle* sh_p = sh_vp;
	const active_sorted_run* asr_p1 = asr_vp1;
	const active_sorted_run* asr_p2 = asr_vp2;
	return compare_tuples(	get_tuple_linked_page_list_iterator(asr_p1->run_iterator), sh_p->std_p->record_def, sh_p->std_p->key_element_ids,
							get_tuple_linked_page_list_iterator(asr_p2->run_iterator), sh_p->std_p->record_def, sh_p->std_p->key_element_ids,
							sh_p->std_p->key_compare_direction,
							sh_p->std_p->key_element_count);
}

// ------------------------------------------------------------------------

// perform singular pass over the sorted runs in the sh_p->sorted_runs_root_page_id, sorting N active runs (N or more runs in a special case) into 1
// external_sort_merge_sorter uses this function until the sorter only consists of lesser than or equal to 1 run
// on an ABORT_ERROR, all iterators including the ones in the sorter_handle are closed
static int merge_sorted_runs_in_sorter(sorter_handle* sh_p, uint64_t N_way, const void* transaction_id, int* abort_error)
{
	// need to merge always 2 or more sorted runs onto 1
	if(N_way < 2)
		return 0;

	// can not merge 1 or lesser number of sorted runs
	if(sh_p->sorted_runs_count <= 1)
		return 0;

	// handle that will be maintained while accessing the runs page_table
	page_table_range_locker* ptrl_p = NULL;
	active_sorted_run_heap input_runs_heap;
	if(0 == initialize_active_sorted_run_heap(&input_runs_heap, N_way))
		exit(-1);
	#define HEAP_DEGREE 2
	#define HEAP_INFO &((heap_info){.type = MIN_HEAP, .comparator = contexted_comparator(sh_p, compare_sorted_runs)})
	active_sorted_run output_run = {};

	uint64_t runs_consumed = 0;		// input runs that have been consumed
	uint64_t runs_created = 0;		// output runs that have been created

	// iterate while all runs have not been consumed
	while(runs_consumed < sh_p->sorted_runs_count)
	{
		// create the input runs, and initialize them into the input_runs_heap, then also initialize the output_run
		// and also update the respective runs_head_page_id in page_table of the sorter accordingly
		{
			ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			uint64_t limit = runs_consumed + N_way;
			for(; runs_consumed < limit && runs_consumed < sh_p->sorted_runs_count; runs_consumed++)
			{
				active_sorted_run e = {};

				e.head_page_id = get_from_page_table(ptrl_p, runs_consumed, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				e.run_iterator = get_new_linked_page_list_iterator(e.head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				push_to_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE, &e);

				set_in_page_table(ptrl_p, runs_consumed, sh_p->std_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}

			output_run.head_page_id = get_new_linked_page_list(&(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			output_run.run_iterator = get_new_linked_page_list_iterator(output_run.head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			set_in_page_table(ptrl_p, runs_created++, output_run.head_page_id, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
			ptrl_p = NULL;
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// iterate while there are still tuples in the runs waiting to be merged into output_run
		while(!is_empty_active_sorted_run_heap(&input_runs_heap))
		{
			// main logic
			{
				// pick a run to consume from
				active_sorted_run e = *get_front_of_active_sorted_run_heap(&input_runs_heap);
				pop_from_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE);

				// move the record from the fron of e to tha tail of output_run
				{
					// record to be consumed
					const void* record = get_tuple_linked_page_list_iterator(e.run_iterator);

					insert_at_linked_page_list_iterator(output_run.run_iterator, record, INSERT_AFTER_LINKED_PAGE_LIST_ITERATOR, transaction_id, abort_error);
					if(*abort_error)
					{
						delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
						goto ABORT_ERROR;
					}

					next_linked_page_list_iterator(output_run.run_iterator, transaction_id, abort_error);
					if(*abort_error)
					{
						delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
						goto ABORT_ERROR;
					}

					remove_from_linked_page_list_iterator(e.run_iterator, GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION, transaction_id, abort_error);
					if(*abort_error)
					{
						delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
						goto ABORT_ERROR;
					}
				}

				// if e is not empty, then we need to put it at the back into the heap
				if(!is_empty_linked_page_list(e.run_iterator))
				{
					push_to_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE, &e);
					continue;
				}

				// else we need to destroy e
				delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
				e.run_iterator = NULL;
				if(*abort_error)
					goto ABORT_ERROR;

				destroy_linked_page_list(e.head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, transaction_id, abort_error );
				if(*abort_error)
					goto ABORT_ERROR;
			}

			// you may not use e beyond this point

			// OPTIMIZATION 2, optimization 1 is under this piece of code
			if(get_element_count_active_sorted_run_heap(&input_runs_heap) == (N_way - 1))
			{
				// Optimization to pick new input run
				// TODO
			}

			// OPTIMIZATION 1
			// merge the only remaining run and break out
			if(get_element_count_active_sorted_run_heap(&input_runs_heap) == 1)
			{
				active_sorted_run run_to_merge = *get_front_of_active_sorted_run_heap(&input_runs_heap);
				pop_from_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE);

				delete_linked_page_list_iterator(run_to_merge.run_iterator, transaction_id, abort_error);
				run_to_merge.run_iterator = NULL;
				if(*abort_error)
					goto ABORT_ERROR;

				delete_linked_page_list_iterator(output_run.run_iterator, transaction_id, abort_error);
				output_run.run_iterator = NULL;
				if(*abort_error)
					goto ABORT_ERROR;

				merge_linked_page_lists(output_run.head_page_id, run_to_merge.head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				break;
			}
		}

		// now we may destroy the iterator over the ouput_run, if it exists
		if(output_run.run_iterator != NULL)
		{
			delete_linked_page_list_iterator(output_run.run_iterator, transaction_id, abort_error);
			output_run = (active_sorted_run){};
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	// update the sorted_runs_count
	sh_p->sorted_runs_count = runs_created;

	return 1;

	ABORT_ERROR:;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	if(ptrl_p != NULL)
		delete_page_table_range_locker(ptrl_p, transaction_id, abort_error);
	while(!is_empty_active_sorted_run_heap(&input_runs_heap))
	{
		active_sorted_run e = *get_front_of_active_sorted_run_heap(&input_runs_heap);
		pop_front_from_active_sorted_run_heap(&input_runs_heap);
		delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
	}
	if(output_run.run_iterator != NULL)
		delete_linked_page_list_iterator(output_run.run_iterator, transaction_id, abort_error);
	return 0;
}

int external_sort_merge_sorter(sorter_handle* sh_p, uint64_t N_way, const void* transaction_id, int* abort_error)
{
	// need to merge always 2 or more sorted runs onto 1
	if(N_way < 2)
		return 0;

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