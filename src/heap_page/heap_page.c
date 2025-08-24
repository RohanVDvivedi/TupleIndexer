#include<tupleindexer/heap_page/heap_page.h>

#include<tupleindexer/heap_page/heap_page_header.h>

persistent_page get_new_heap_page_with_write_lock(const page_access_specs* pas_p, const tuple_def* tpl_d, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page heap_page = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);
	if(*abort_error)
		return heap_page;

	int inited = init_persistent_page(pmm_p, transaction_id, &heap_page, pas_p->page_size, sizeof_HEAP_PAGE_HEADER(pas_p), &(tpl_d->size_def), abort_error);
	if((*abort_error) || !inited)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, NONE_OPTION, abort_error);
		return heap_page;
	}

	// get the header, initialize it and set it back on to the page
	heap_page_header hdr = get_heap_page_header(&heap_page, pas_p);
	hdr.parent.type = HEAP_PAGE; // always a heap page
	set_heap_page_header(&heap_page, &hdr, pas_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, NONE_OPTION, abort_error);
		return heap_page;
	}

	return heap_page;
}

uint32_t get_unused_space_on_heap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d)
{
	uint32_t unused_space = get_space_allotted_to_all_tuples_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def)) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def));

	// but there is more to it
	// any heap_page insertion can replace a tombstones, so add tombstone to unused_space

	if(is_fixed_sized_tuple_def(tpl_d))
	{
		// add tombstones to unused_space
		unused_space += (get_minimum_tuple_size(tpl_d) * get_tomb_stone_count_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def)));
	}
	else
	{
		// else add back 1 additional_space worth of memory if there are any tombstones on the page
		// because on a slotted page, any update to a tombstone can only use up atmost 1 tombstone and the rest tombstones remain unaffected
		if(get_tomb_stone_count_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def)) > 0)
		{
			unused_space += get_additional_space_overhead_per_tuple_on_persistent_page(pas_p->page_size, &(tpl_d->size_def));
		}
	}

	return unused_space;
}

int is_heap_page_empty(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d)
{
	return get_tomb_stone_count_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def)) == 
			get_tuple_count_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def));
}

uint32_t insert_in_heap_page(persistent_page* ppage, const void* tuple, uint32_t* possible_insertion_index, const tuple_def* tpl_d, const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// get tuple_count on the page
	uint32_t tuple_count = get_tuple_count_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def));

	// limit the (*possible_insertion_index) to be not more than the tuple_count
	(*possible_insertion_index) = min((*possible_insertion_index), tuple_count);

	// iterate over all the remaining tuples on the page
	for(; (*possible_insertion_index) < tuple_count; (*possible_insertion_index)++)
	{
		// if a tuple exists at (*possible_insertion_index), then continue and look for the next one
		if(exists_tuple_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def), (*possible_insertion_index)))
			continue;

		// perform an update at that (*possible_insertion_index), where presently there is no tuple, and holds just a tombstone
		int inserted = update_tuple_on_persistent_page_resiliently(pmm_p, transaction_id, ppage, pas_p->page_size, &(tpl_d->size_def), (*possible_insertion_index), tuple, abort_error);
		if(*abort_error) // if aborted, fail this call
			return INVALID_TUPLE_INDEX;
		if(inserted) // if inserted, we are done, return the current (*possible_insertion_index) and then increment it
			return (*possible_insertion_index)++;
		else // else this page is not big enough, but we found an empty slot
			return INVALID_TUPLE_INDEX;
	}

	// if we reach here (*possible_insertion_index) = tuple_count

	// perfrom an append now
	int inserted = append_tuple_on_persistent_page_resiliently(pmm_p, transaction_id, ppage, pas_p->page_size, &(tpl_d->size_def), tuple, abort_error);
	if(*abort_error) // if aborted, fail this call
		return INVALID_TUPLE_INDEX;
	if(inserted) // if inserted, we are done, return the current (*possible_insertion_index) and then increment it
		return (*possible_insertion_index)++;
	else // else this page is not big enough
		return INVALID_TUPLE_INDEX;
}

int delete_from_heap_page(persistent_page* ppage, uint32_t index, const tuple_def* tpl_d, const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// first update the index in concern to NULL
	int removed = update_tuple_on_persistent_page_resiliently(pmm_p, transaction_id, ppage, pas_p->page_size, &(tpl_d->size_def), index, NULL, abort_error);
	if(*abort_error)
		return 0;
	if(!removed) // if not removed, fail this call
		return 0;

	// then discard trailing tombstones
	discard_trailing_tomb_stones_on_persistent_page(pmm_p, transaction_id, ppage, pas_p->page_size, &(tpl_d->size_def), abort_error);
	if(*abort_error)
		return 0;

	return removed;
}

void print_heap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d)
{
	print_heap_page_header(ppage, pas_p);
	print_persistent_page(ppage, pas_p->page_size, tpl_d);
}