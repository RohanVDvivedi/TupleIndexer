#include<tupleindexer/sorter/sorter.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/linked_page_list/linked_page_list.h>

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

	sh.sorted_runs_first_index = 0;
	sh.sorted_runs_count = 0;
	sh.sorted_runs_slots_reserved_count = 0;

	// create a page_table for holding the sorted runs
	// failure here is reported by the abort_error if any, else it is a success
	sh.sorted_runs_root_page_id = get_new_page_table(&(std_p->pttd), pam_p, pmm_p, transaction_id, abort_error);

	return sh;
}

// produces (i1 + i2) % SORTED_RUNS_CAPACITY
static inline uint64_t add_circular(uint64_t i1, uint64_t i2)
{
	if(i1 == SORTED_RUNS_CAPACITY)
		i1 = 0;
	if(i2 == SORTED_RUNS_CAPACITY)
		i2 = 0;

	if(SORTED_RUNS_CAPACITY - i1 > i2)
		return i1 + i2;
	else
		return i2 - (SORTED_RUNS_CAPACITY - i1);
}

/*
	below 2 functions are purely internal and you must not attempt to call them from outside
	reserve_count_* functions must be lesser than the value of (*run_head_page_ids_count), else it is a bug
	if you reserve a number of slots by using a reserve_count_increment parameter then it implies you will return this same number of sorted_runs upon completion of your task (of merging these sorted runs that you just popped)
	basically you reserve slots during pop, so that your next anticipated push succeeds
*/

// internal function to pop sorted runs
static inline uint64_t pop_sorted_runs(sorter_handle* sh_p, uint64_t* run_head_page_ids, uint32_t run_head_page_ids_count, uint32_t reserve_count_increment, const void* transaction_id, int* abort_error)
{
	// handle that will have to be freed/destroyed on error or on return
	page_table_range_locker* ptrl_p = NULL;

	// return value
	uint64_t popped_count = 0;

	{
		ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// you can never pop more runs than what the sorted_runs actually contains
		popped_count = min(sh_p->sorted_runs_count, run_head_page_ids_count);

		// you can not reserve more slots than what you pop
		reserve_count_increment = min(reserve_count_increment, popped_count);

		for(uint64_t i = 0; i < popped_count; i++)
		{
			uint64_t index = add_circular(sh_p->sorted_runs_first_index, i);

			run_head_page_ids[i] = get_from_page_table(ptrl_p, index, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			set_in_page_table(ptrl_p, index, sh_p->std_p->pttd.pas_p->NULL_PAGE_ID, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		sh_p->sorted_runs_first_index = add_circular(sh_p->sorted_runs_first_index, popped_count);
		sh_p->sorted_runs_count -= popped_count;
		sh_p->sorted_runs_slots_reserved_count += reserve_count_increment;

		delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, abort_error);
		ptrl_p = NULL;
		if(*abort_error)
			goto ABORT_ERROR;
	}

	return popped_count;

	// all you need to do here is clean up all the open iterators
	ABORT_ERROR:;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	sh_p->unsorted_partial_run = NULL;
	if(ptrl_p != NULL)
		delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, abort_error); // we have lock on the WHOLE_BUCKET_RANGE, hence vaccum not needed
	return 0;
}

// internal function to push sorted runs
static inline uint64_t push_sorted_runs(sorter_handle* sh_p, uint64_t* run_head_page_ids, uint32_t run_head_page_ids_count, int were_reserved_slots, const void* transaction_id, int* abort_error)
{
	// handle that will have to be freed/destroyed on error or on return
	page_table_range_locker* ptrl_p = NULL;

	// return value
	uint64_t pushed_count = 0;

	{
		ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		if(were_reserved_slots)
		{
			if(sh_p->sorted_runs_slots_reserved_count < run_head_page_ids_count)
			{
				printf("BUG :: pushing to reserved slots over the current reservation in sorter\n");
				exit(-1);
			}

			pushed_count = run_head_page_ids_count;
		}
		else
		{
			uint64_t total_pushable_slots = SORTED_RUNS_CAPACITY - (sh_p->sorted_runs_count + sh_p->sorted_runs_slots_reserved_count);
			pushed_count = min(run_head_page_ids_count, total_pushable_slots);
		}

		for(uint64_t i = 0; i < pushed_count; i++)
		{
			uint64_t index = add_circular(add_circular(sh_p->sorted_runs_first_index, sh_p->sorted_runs_count), i);

			set_in_page_table(ptrl_p, index, run_head_page_ids[i], transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		sh_p->sorted_runs_count += pushed_count;
		if(were_reserved_slots)
			sh_p->sorted_runs_slots_reserved_count -= pushed_count;

		delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, abort_error);
		ptrl_p = NULL;
		if(*abort_error)
			goto ABORT_ERROR;
	}

	return pushed_count;

	// all you need to do here is clean up all the open iterators
	ABORT_ERROR:;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	sh_p->unsorted_partial_run = NULL;
	if(ptrl_p != NULL)
		delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, abort_error); // we have lock on the WHOLE_BUCKET_RANGE, hence vaccum not needed
	return 0;
}

// if there exists a unsorted partial run, then it is sorted and put at the end of the sorted runs
// ensure that the unsorted_partial_run exists, and it is not empty and as a HEAD_ONLY_LINKED_PAGE_LIST
// on an ABORT_ERROR, all iterators including the ones in the sorter_handle are closed
static int consume_unsorted_partial_run_from_sorter(sorter_handle* sh_p, const void* transaction_id, int* abort_error)
{
	// fail if there is no unsorted_partial_run
	if(sh_p->unsorted_partial_run == NULL)
		return 0;

	// ensure that the unsorted_partial is an ONLY_HEAD_LINKED_PAGE_LIST and is not empty
	if(is_empty_linked_page_list(sh_p->unsorted_partial_run) || HEAD_ONLY_LINKED_PAGE_LIST != get_state_for_linked_page_list(sh_p->unsorted_partial_run))
		return 0;

	// sort the unsorted_partial_run
	//sort_all_tuples_on_curr_page_in_linked_page_list_iterator(sh_p->unsorted_partial_run, sh_p->std_p->key_element_ids, sh_p->std_p->key_compare_direction, sh_p->std_p->key_element_count, transaction_id, abort_error);
	sort_materialized_all_tuples_on_curr_page_in_linked_page_list_iterator(sh_p->unsorted_partial_run, sh_p->std_p->key_type_infos, sh_p->std_p->key_element_ids, sh_p->std_p->key_compare_direction, sh_p->std_p->key_element_count, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// push it to the back of the sorted_runs queue
	uint64_t pushed = push_sorted_runs(sh_p, ((uint64_t[1]){sh_p->unsorted_partial_run_head_page_id}), 1, 0, transaction_id, abort_error);
	if(*abort_error) // if aborted after this call, the sorter is already freed from it's resources
		return 0;
	if(pushed == 0) // if not pushed fail here
		return 0;

	// close the iterator on unsorted_partial_run
	delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	sh_p->unsorted_partial_run = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	// reinitialize the head_page_id of unsorted_partial_run
	sh_p->unsorted_partial_run_head_page_id = sh_p->std_p->lpltd.pas_p->NULL_PAGE_ID;

	return 1;

	// all you need to do here is clean up all the open iterators
	ABORT_ERROR:;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	sh_p->unsorted_partial_run = NULL;
	return 0;
}

int insert_in_sorter(sorter_handle* sh_p, const void* record, const void* transaction_id, int* abort_error)
{
	// giving this function a NULL record implies that all the user wants is to mark finish for his insertions
	if(record == NULL)
		return consume_unsorted_partial_run_from_sorter(sh_p, transaction_id, abort_error);

	// check that the record could be inserted to the runs (linked_page_list)
	// this check will fail insertion even if record == NULL
	if(!check_if_record_can_be_inserted_for_sorter_tuple_definitions(sh_p->std_p, record))
		return 0;

	// if the unsorted_partial_run is full, then consume all the tuples of the unsorted_partial_run
	// sh_p->unsorted_partial_run may be equal to NULL, if the sorter_handle has just been initialized or sorted using external_sort_merge_sorter
	if(sh_p->unsorted_partial_run != NULL && !can_insert_without_split_at_linked_page_list_iterator(sh_p->unsorted_partial_run, record))
	{
		int consumed = consume_unsorted_partial_run_from_sorter(sh_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		if(!consumed)
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
	sh_p->unsorted_partial_run = NULL;
	return 0;
}

// below is the utility required to work with merge_sorted_runs_in_sorter

typedef struct active_sorted_run active_sorted_run;
struct active_sorted_run
{
	uint64_t head_page_id;
	linked_page_list_iterator* run_iterator;

	datum* cached_keys_for_curr_tuple;
};

static void cache_keys_for_active_sorted_run(active_sorted_run* asr_p, const sorter_tuple_defs* std_p)
{
	// allocate memory for caches_keys
	if(asr_p->cached_keys_for_curr_tuple == NULL)
	{
		asr_p->cached_keys_for_curr_tuple = malloc(sizeof(datum) * std_p->key_element_count);
		if(asr_p->cached_keys_for_curr_tuple == NULL)
			exit(-1);
	}

	const void* tuple = get_tuple_linked_page_list_iterator(asr_p->run_iterator);

	for(uint32_t i = 0; i < std_p->key_element_count; i++)
		get_value_from_element_from_tuple(&(asr_p->cached_keys_for_curr_tuple[i]), std_p->record_def, std_p->key_element_ids[i], tuple);
}

static void destroy_cache_keys_for_active_sorted_run(active_sorted_run* asr_p)
{
	if(asr_p->cached_keys_for_curr_tuple != NULL)
	{
		free(asr_p->cached_keys_for_curr_tuple);
		asr_p->cached_keys_for_curr_tuple = NULL;
	}
}

#include<cutlery/value_arraylist.h>

data_definitions_value_arraylist(active_sorted_run_heap, active_sorted_run)
declarations_value_arraylist(active_sorted_run_heap, active_sorted_run, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(active_sorted_run_heap, active_sorted_run, static inline)

static int compare_sorted_runs(const void* sh_vp, const void* asr_vp1, const void* asr_vp2)
{
	const sorter_handle* sh_p = sh_vp;
	const active_sorted_run* asr_p1 = asr_vp1;
	const active_sorted_run* asr_p2 = asr_vp2;
	if(asr_p1->cached_keys_for_curr_tuple != NULL && asr_p2->cached_keys_for_curr_tuple != NULL)
		return compare_datums3(asr_p1->cached_keys_for_curr_tuple,
									asr_p2->cached_keys_for_curr_tuple,
									sh_p->std_p->key_type_infos,
									sh_p->std_p->key_compare_direction,
									sh_p->std_p->key_element_count);
	else
		return compare_tuples(	get_tuple_linked_page_list_iterator(asr_p1->run_iterator), sh_p->std_p->record_def, sh_p->std_p->key_element_ids,
								get_tuple_linked_page_list_iterator(asr_p2->run_iterator), sh_p->std_p->record_def, sh_p->std_p->key_element_ids,
								sh_p->std_p->key_compare_direction,
								sh_p->std_p->key_element_count);
}

// ------------------------------------------------------------------------

int merge_few_run_in_sorter(sorter_handle* sh_p, uint32_t N_way, const void* transaction_id, int* abort_error)
{
	// need to merge always 2 or more sorted runs onto 1
	if(N_way < 2)
		return 0;

	// can not merge 1 or lesser number of sorted runs
	if(sh_p->sorted_runs_count <= 1)
		return 0;

	uint64_t* sorted_runs_page_ids = NULL;
	active_sorted_run_heap input_runs_heap;
	if(0 == initialize_active_sorted_run_heap(&input_runs_heap, min(N_way, sh_p->sorted_runs_count)))
		exit(-1);
	#define HEAP_DEGREE 2
	#define HEAP_INFO &((heap_info){.type = MIN_HEAP, .comparator = contexted_comparator(sh_p, compare_sorted_runs)})
	active_sorted_run output_run = {};

	// iterate while all runs have not been consumed
	{
		uint64_t* sorted_runs_page_ids = malloc(sizeof(uint64_t) * N_way);
		if(sorted_runs_page_ids == NULL)
			exit(-1);

		N_way = pop_sorted_runs(sh_p, sorted_runs_page_ids, N_way, 1, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		if(N_way == 1)
		{
			push_sorted_runs(sh_p, sorted_runs_page_ids, 1, 1, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			free(sorted_runs_page_ids);
			return 0;
		}

		for(uint64_t i = 0; i < N_way; i++)
		{
			active_sorted_run e = {};

			e.head_page_id = sorted_runs_page_ids[i];

			e.run_iterator = get_new_linked_page_list_iterator(e.head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			cache_keys_for_active_sorted_run(&e, sh_p->std_p);
			push_to_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE, &e);
		}

		free(sorted_runs_page_ids);
		sorted_runs_page_ids = NULL;

		output_run.head_page_id = get_new_linked_page_list(&(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		output_run.run_iterator = get_new_linked_page_list_iterator(output_run.head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		// iterate while there are still tuples in the runs waiting to be merged into output_run
		while(!is_empty_active_sorted_run_heap(&input_runs_heap))
		{
			// main logic
			{
				// pick a run to consume from
				active_sorted_run e = *get_front_of_active_sorted_run_heap(&input_runs_heap);
				pop_from_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE);

				// move the record from the front of e to tha tail of output_run
				{
					// record to be consumed
					const void* record = get_tuple_linked_page_list_iterator(e.run_iterator);

					insert_at_linked_page_list_iterator(output_run.run_iterator, record, INSERT_AFTER_LINKED_PAGE_LIST_ITERATOR, transaction_id, abort_error);
					if(*abort_error)
					{
						destroy_cache_keys_for_active_sorted_run(&e);
						delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
						goto ABORT_ERROR;
					}

					next_linked_page_list_iterator(output_run.run_iterator, transaction_id, abort_error);
					if(*abort_error)
					{
						destroy_cache_keys_for_active_sorted_run(&e);
						delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
						goto ABORT_ERROR;
					}

					if(!is_at_last_tuple_in_curr_page_linked_page_list_iterator(e.run_iterator)) // if it is not at the last tuple in curr page, then simply go next
					{
						next_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
						if(*abort_error)
						{
							destroy_cache_keys_for_active_sorted_run(&e);
							delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
							goto ABORT_ERROR;
						}
					}
					else // if it is at the last tuple in curr page, then the whole page worth of tuples can be discarded
					{
						remove_all_in_curr_page_from_linked_page_list_iterator(e.run_iterator, GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION, transaction_id, abort_error);
						if(*abort_error)
						{
							destroy_cache_keys_for_active_sorted_run(&e);
							delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
							goto ABORT_ERROR;
						}
					}
				}

				// if e is not empty, then we need to put it at the back into the heap
				if(!is_empty_linked_page_list(e.run_iterator))
				{
					cache_keys_for_active_sorted_run(&e, sh_p->std_p);
					push_to_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE, &e);
					continue;
				}

				// e is empty, so it is meant to be destroyed

				// else we need to destroy e
				destroy_cache_keys_for_active_sorted_run(&e);
				delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
				e.run_iterator = NULL;
				if(*abort_error)
					goto ABORT_ERROR;

				destroy_linked_page_list(e.head_page_id, &(sh_p->std_p->lpltd), sh_p->pam_p, transaction_id, abort_error );
				if(*abort_error)
					goto ABORT_ERROR;
			}

			// you may not use e beyond this point

			// merge the only remaining run and break out
			// this is to be performed only if we are at the head of the last remaining run, else iterate on the current page tuple by tuple
			if(get_element_count_active_sorted_run_heap(&input_runs_heap) == 1 && is_at_head_tuple_linked_page_list_iterator(((active_sorted_run*)get_front_of_active_sorted_run_heap(&input_runs_heap))->run_iterator))
			{
				active_sorted_run run_to_merge = *get_front_of_active_sorted_run_heap(&input_runs_heap);
				pop_from_heap_active_sorted_run_heap(&input_runs_heap, HEAP_INFO, HEAP_DEGREE);

				destroy_cache_keys_for_active_sorted_run(&run_to_merge);
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

		delete_linked_page_list_iterator(output_run.run_iterator, transaction_id, abort_error);
		output_run.run_iterator = NULL;
		if(*abort_error)
			goto ABORT_ERROR;

		push_sorted_runs(sh_p, &(output_run.head_page_id), 1, 1, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// destroy the heap
	deinitialize_active_sorted_run_heap(&input_runs_heap);

	return 1;

	ABORT_ERROR:;
	if(sorted_runs_page_ids != NULL)
		free(sorted_runs_page_ids);
	sorted_runs_page_ids = NULL;
	if(sh_p->unsorted_partial_run != NULL)
		delete_linked_page_list_iterator(sh_p->unsorted_partial_run, transaction_id, abort_error);
	sh_p->unsorted_partial_run = NULL;
	while(!is_empty_active_sorted_run_heap(&input_runs_heap))
	{
		active_sorted_run e = *get_front_of_active_sorted_run_heap(&input_runs_heap);
		pop_front_from_active_sorted_run_heap(&input_runs_heap);
		destroy_cache_keys_for_active_sorted_run(&e);
		delete_linked_page_list_iterator(e.run_iterator, transaction_id, abort_error);
	}
	deinitialize_active_sorted_run_heap(&input_runs_heap);
	if(output_run.run_iterator != NULL)
		delete_linked_page_list_iterator(output_run.run_iterator, transaction_id, abort_error);
	return 0;
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
		ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, sh_p->pmm_p, transaction_id, abort_error);
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

		delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, abort_error); // we have lock on the WHOLE_BUCKET_RANGE, hence vaccum not needed
		ptrl_p = NULL;
		if(*abort_error)
			goto ABORT_ERROR;
	}

	// destroy the runs page_table, and the contained runs must also be destroyed
	{
		ptrl_p = get_new_page_table_range_locker(sh_p->sorted_runs_root_page_id, WHOLE_BUCKET_RANGE, &(sh_p->std_p->pttd), sh_p->pam_p, NULL, transaction_id, abort_error);
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

		delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, abort_error); // we have lock on the WHOLE_BUCKET_RANGE, hence vaccum not needed
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
		delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, abort_error); // we have lock on the WHOLE_BUCKET_RANGE, hence vaccum not needed
	return 0;
}