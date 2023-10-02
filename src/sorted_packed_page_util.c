#include<sorted_packed_page_util.h>

#include<tuple.h>
#include<page_layout_unaltered.h>

#include<index_accessed_interface.h>
#include<index_accessed_search_sort.h>

// ---------------- UTILTY CODE FOR SORTED PACKED PAGE BEGIN -------------------------------------------------

typedef struct tuple_on_page_compare_context tuple_on_page_compare_context;
struct tuple_on_page_compare_context
{
	// tuple_def for all tuples to be compared that are on the page
	const tuple_def* tpl_def;

	// keys of the tuple on the page to compare on
	const uint32_t* tuple_keys_to_compare;

	// tuple_def for the 2nd parameter of the comparator, this must equal tpl_def when not in use
	const tuple_def* key_def;

	// keys of the key tuple, this must equal tuple_keys_to_compare when not in use
	const uint32_t* key_elements_to_compare;

	// number of elements in tuple_keys_to_compare and key_elements_to_compare
	uint32_t keys_count;
};

#define get_tuple_on_page_compare_context(tpl_def_v, tuple_keys_to_compare_v, key_def_v, key_elements_to_compare_v, keys_count_v) ((const tuple_on_page_compare_context){.tpl_def = tpl_def_v, .tuple_keys_to_compare = tuple_keys_to_compare_v, .key_def = key_def_v, .key_elements_to_compare = key_elements_to_compare_v, .keys_count = keys_count_v})

int compare_tuples_using_comparator_context(const void* context, const void* tuple1, const void* tuple2)
{
	const tuple_on_page_compare_context* context_p = context;
	return compare_tuples(tuple1, context_p->tpl_def, context_p->tuple_keys_to_compare, tuple2, context_p->key_def, context_p->key_elements_to_compare, context_p->keys_count);
}

typedef struct tuple_accessed_page tuple_accessed_page;
struct tuple_accessed_page
{
	// the actual persistent page
	persistent_page ppage;

	// the size of the page
	uint32_t page_size;

	// tuple_def to be used with the page
	const tuple_def* tpl_def;

	// page_modification_methods allowing you to modify tuples on page
	// only required for swapping tuples
	const page_modification_methods* pmm_p;
};

static const void* get_tuple_from_tuple_accessed_page(const void* ds_p, cy_uint index)
{
	const tuple_accessed_page* tap_p = ds_p;
	return get_nth_tuple_on_page(tap_p->ppage.page, tap_p->page_size, &(tap_p->tpl_def->size_def), index);
}

// set functionality will not be provided

static int swap_tuples_in_tuple_accessed_page(void* ds_p, cy_uint i1, cy_uint i2)
{
	tuple_accessed_page* tap_p = ds_p;
	return tap_p->pmm_p->swap_tuples_on_page(tap_p->pmm_p->context, tap_p->ppage, tap_p->page_size, &(tap_p->tpl_def->size_def), i1, i2);
}

static cy_uint get_tuple_count_for_tuple_accessed_page(const void* ds_p)
{
	const tuple_accessed_page* tap_p = ds_p;
	return get_tuple_count_on_page(tap_p->ppage.page, tap_p->page_size, &(tap_p->tpl_def->size_def));
}

#define get_tuple_accessed_page(ppage_v, page_size_v, tpl_def_v, pmm_p_v) ((const tuple_accessed_page){.ppage = ppage_v, .page_size = page_size_v, .tpl_def = tpl_def_v, .pmm_p = pmm_p_v})

index_accessed_interface get_index_accessed_interface_for_sorted_packed_page(tuple_accessed_page* tap_p)
{
	return (index_accessed_interface) {
		.ds_p = tap_p,
		.get_element = get_tuple_from_tuple_accessed_page,
		.set_element = NULL, // set element functionality will not be provided
		.swap_elements = (tap_p->pmm_p == NULL) ? NULL : swap_tuples_in_tuple_accessed_page, // you are not suppossed to modify a persistent page if pmm_p is NULL
		.get_element_count = get_tuple_count_for_tuple_accessed_page
	};
}

// ---------------- UTILTY CODE FOR SORTED PACKED PAGE END ---------------------------------------------------

// the 0 <= index <= tuple_count
// internal function to insert a tuple at a specified index
// this function does not check that the sort order is maintained
static int insert_at_in_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def,
									const void* tuple, 
									uint32_t index,
									const page_modification_methods* pmm_p
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(ppage.page, page_size, &(tpl_def->size_def));

	// if the index is not valid we fail the insertion
	if( !(0 <= index && index <= tuple_count) )
		return 0;

	// insert tuple to the end of the page
	if(!(pmm_p->append_tuple_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), tuple)))
	{
		// if it fails the first time then calculate unused space on the page and the space required by the tuple
		uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_page(ppage.page, page_size, &(tpl_def->size_def)) - get_space_occupied_by_all_tuples_on_page(ppage.page, page_size, &(tpl_def->size_def));
		uint32_t space_required_by_tuple = get_tuple_size(tpl_def, tuple) + get_additional_space_overhead_per_tuple_on_page(page_size, &(tpl_def->size_def));

		// if page does not have enough unused space then we can't do anything
		if(unused_space_on_page < space_required_by_tuple)
			return 0;

		// else run_page_compaction and try again
		run_page_compaction(ppage.page, page_size, &(tpl_def->size_def));

		if(!(pmm_p->append_tuple_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), tuple)))
			return 0;
	}

	// insert succeedded, so tuple_count incremented
	tuple_count++;

	// curr_index that the tuple is at
	uint32_t curr_index = tuple_count - 1;

	// swap until the new tuple is not at index
	while(index != curr_index)
	{
		pmm_p->swap_tuples_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), curr_index - 1, curr_index);
		curr_index--;
	}

	return 1;
}

int insert_to_sorted_packed_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t* index,
									const page_modification_methods* pmm_p
								)
{
	// search for a viable index for the new tuple to insert
	uint32_t new_index = find_insertion_point_in_sorted_packed_page(ppage.page, page_size, tpl_def, tuple_keys_to_compare, keys_count, tuple);

	// this is the final index for the newly inserted element
	if(index != NULL)
		(*index) = new_index;

	// insert tuple to the page at the desired index
	return insert_at_in_page(ppage, page_size, tpl_def, tuple, new_index, pmm_p);
}

int insert_at_in_sorted_packed_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index,
									const page_modification_methods* pmm_p
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(ppage.page, page_size, &(tpl_def->size_def));

	// if the index is not valid we fail the insertion
	if( !(0 <= index && index <= tuple_count) )
		return 0;

	// in the below conditions we ensure that the tuple order is maintained

	// the tuple compares greater than the tuple at index, we fail
	if(tuple_count > 0 && index < tuple_count)
	{
		const void* ith_tuple = get_nth_tuple_on_page(ppage.page, page_size, &(tpl_def->size_def), index);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, ith_tuple, tpl_def, tuple_keys_to_compare, keys_count) > 0)
			return 0;
	}

	// the tuple compares lesser than the tuple at (index - 1), we fail
	if(tuple_count > 0 && index > 0)
	{
		const void* i_1_th_tuple = get_nth_tuple_on_page(ppage.page, page_size, &(tpl_def->size_def), index - 1);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, i_1_th_tuple, tpl_def, tuple_keys_to_compare, keys_count) < 0)
			return 0;
	}

	// insert tuple to the page at the desired index
	return insert_at_in_page(ppage, page_size, tpl_def, tuple, index, pmm_p);
}

int update_resiliently_at_in_sorted_packed_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index,
									const page_modification_methods* pmm_p
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(ppage.page, page_size, &(tpl_def->size_def));

	// if the index is not valid we fail the update
	if( !(0 <= index && index < tuple_count) )
		return 0;

	// in the below conditions we ensure that the tuple order is maintained

	// the tuple compares greater than the tuple at (index + 1), we fail
	if(tuple_count > 0 && index != tuple_count - 1)
	{
		const void* i_1_th_tuple = get_nth_tuple_on_page(ppage.page, page_size, &(tpl_def->size_def), index + 1);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, i_1_th_tuple, tpl_def, tuple_keys_to_compare, keys_count) > 0)
			return 0;
	}

	// the tuple compares lesser than the tuple at (index - 1), we fail
	if(tuple_count > 0 && index > 0)
	{
		const void* i_1_th_tuple = get_nth_tuple_on_page(ppage.page, page_size, &(tpl_def->size_def), index - 1);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, i_1_th_tuple, tpl_def, tuple_keys_to_compare, keys_count) < 0)
			return 0;
	}

	int update_successfull = pmm_p->update_tuple_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), index, tuple);

	// if simple update was successfull (without defragmenting and discarding tombstones)
	if(update_successfull)
		return update_successfull;

	// get free space on page after it gets defragmented
	// here we assume that the page passed to this functions has no tombstones as should be the case with sorted_packed_page
	uint32_t free_space_after_defragmentation = get_free_space_on_page(ppage.page, page_size, &(tpl_def->size_def)) + get_fragmentation_space_on_page(ppage.page, page_size, &(tpl_def->size_def));

	// get size of existing tuple (at index = index)
	const void* existing_tuple = get_nth_tuple_on_page(ppage.page, page_size, &(tpl_def->size_def), index);
	uint32_t existing_tuple_size = get_tuple_size(tpl_def, existing_tuple);

	// if discarding the existing tuple can make enough room for the new tuple then
	if(free_space_after_defragmentation + existing_tuple_size >= get_tuple_size(tpl_def, tuple))
	{
		// place tomb_stone for the old tuple at the index
		pmm_p->update_tuple_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), index, NULL);

		// defragment the page
		run_page_compaction(ppage.page, page_size, &(tpl_def->size_def));

		// then at the end attempt to update the tuple again
		// this time it must succeed
		update_successfull = pmm_p->update_tuple_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), index, tuple);
	}

	return update_successfull;
}

int delete_in_sorted_packed_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def,
									uint32_t index,
									const page_modification_methods* pmm_p
								)
{
	return pmm_p->discard_tuple_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), index);
}

int delete_all_in_sorted_packed_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def, 
									uint32_t start_index, uint32_t last_index,
									const page_modification_methods* pmm_p
								)
{
	uint32_t count = get_tuple_count_on_page(ppage.page, page_size, &(tpl_def->size_def));
	if(count == 0 || start_index > last_index || last_index >= count)
		return 0;

	// if the user wants to discard all tuple, do it quickly using
	if(start_index == 0 && last_index == count - 1)
	{
		pmm_p->discard_all_tuples_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def));
		return 1;
	}

	for(uint32_t i = start_index; i <= last_index; i++)
		pmm_p->discard_tuple_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), start_index); // a discarded tuple does not leave a slot, hence always discarding at start_index

	return 1;
}

static uint32_t append_tuples_from_page(persistent_page ppage_dest, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page ppage_src, uint32_t start_index, uint32_t last_index, const page_modification_methods* pmm_p)
{
	uint32_t appended_count = 0;
	for(uint32_t i = start_index; i <= last_index; i++)
	{
		const void* tup = get_nth_tuple_on_page(ppage_src.page, page_size, tpl_sz_d, i);
		if(tup == NULL)
			continue;

		int res = pmm_p->append_tuple_on_page(pmm_p->context, ppage_dest, page_size, tpl_sz_d, tup);
		if(res == 0)
			break;

		appended_count++;
	}

	return appended_count;
}

uint32_t insert_all_from_sorted_packed_page(
									persistent_page ppage_dest, persistent_page ppage_src, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									uint32_t start_index, uint32_t last_index,
									const page_modification_methods* pmm_p
								)
{
	uint32_t src_count = get_tuple_count_on_page(ppage_src.page, page_size, &(tpl_def->size_def));
	if(src_count == 0 || start_index > last_index || last_index >= src_count)
		return 0;

	// if the dest page is empty, insert all no comparisons needed
	uint32_t dest_count = get_tuple_count_on_page(ppage_dest.page, page_size, &(tpl_def->size_def));
	if(dest_count == 0)
		return append_tuples_from_page(ppage_dest, page_size, &(tpl_def->size_def), ppage_src, start_index, last_index, pmm_p);

	// compare the last tuple of the dest page and first tuple of the src page
	const void* last_tuple_dest = get_nth_tuple_on_page(ppage_dest.page, page_size, &(tpl_def->size_def), dest_count - 1);
	const void* first_tuple_src = get_nth_tuple_on_page(ppage_src.page, page_size, &(tpl_def->size_def), 0);

	// if they are in order then perform a direct copy
	int compare_last_first = compare_tuples(last_tuple_dest, tpl_def, tuple_keys_to_compare, first_tuple_src, tpl_def, tuple_keys_to_compare, keys_count);
	if(compare_last_first <= 0)
		return append_tuples_from_page(ppage_dest, page_size, &(tpl_def->size_def), ppage_src, start_index, last_index, pmm_p);

	uint32_t inserted_count = 0;

	// insert one by one from start_index to last_index
	for(uint32_t index = start_index; index <= last_index; index++)
	{
		const void* tup = get_nth_tuple_on_page(ppage_src.page, page_size, &(tpl_def->size_def), index);
		if(tup == NULL)
			continue;

		int res = insert_to_sorted_packed_page(ppage_dest, page_size, tpl_def, tuple_keys_to_compare, keys_count, tup, NULL, pmm_p);
		if(res == 0)
			break;

		inserted_count++;
	}

	return inserted_count;
}

uint32_t find_insertion_point_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple
									)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the page is empty insert it at 0
	if(tuple_count == 0)
		return 0;

	tuple_accessed_page tap = get_tuple_accessed_page(((persistent_page){.page = ((void*)page)}), page_size, tpl_def, NULL);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, tpl_def, tuple_keys_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	return find_insertion_index_in_sorted_iai(&iai, 0, tuple_count - 1, tuple, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));
}

uint32_t find_first_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the page is empty
	if(tuple_count == 0)
		return NO_TUPLE_FOUND;

	tuple_accessed_page tap = get_tuple_accessed_page(((persistent_page){.page = ((void*)page)}), page_size, tpl_def, NULL);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	cy_uint result = binary_search_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context), FIRST_OCCURENCE);

	// NO_TUPLE_FOUND case
	if(result == INVALID_INDEX)
		return NO_TUPLE_FOUND;

	return result;
}

uint32_t find_last_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the page is empty
	if(tuple_count == 0)
		return NO_TUPLE_FOUND;

	tuple_accessed_page tap = get_tuple_accessed_page(((persistent_page){.page = ((void*)page)}), page_size, tpl_def, NULL);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	cy_uint result = binary_search_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context), LAST_OCCURENCE);

	// NO_TUPLE_FOUND case
	if(result == INVALID_INDEX)
		return NO_TUPLE_FOUND;

	return result;
}

uint32_t find_preceding_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the page is empty
	if(tuple_count == 0)
		return NO_TUPLE_FOUND;

	tuple_accessed_page tap = get_tuple_accessed_page(((persistent_page){.page = ((void*)page)}), page_size, tpl_def, NULL);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	cy_uint result = find_preceding_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));

	// NO_TUPLE_FOUND case
	if(result == INVALID_INDEX)
		return NO_TUPLE_FOUND;

	return result;
}

uint32_t find_preceding_equals_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the page is empty
	if(tuple_count == 0)
		return NO_TUPLE_FOUND;

	tuple_accessed_page tap = get_tuple_accessed_page(((persistent_page){.page = ((void*)page)}), page_size, tpl_def, NULL);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	cy_uint result = find_preceding_or_equals_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));

	// NO_TUPLE_FOUND case
	if(result == INVALID_INDEX)
		return NO_TUPLE_FOUND;

	return result;
}

uint32_t find_succeeding_equals_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the page is empty
	if(tuple_count == 0)
		return NO_TUPLE_FOUND;

	tuple_accessed_page tap = get_tuple_accessed_page(((persistent_page){.page = ((void*)page)}), page_size, tpl_def, NULL);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	cy_uint result = find_succeeding_or_equals_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));

	// NO_TUPLE_FOUND case
	if(result == INVALID_INDEX)
		return NO_TUPLE_FOUND;

	return result;
}

uint32_t find_succeeding_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the page is empty
	if(tuple_count == 0)
		return NO_TUPLE_FOUND;

	tuple_accessed_page tap = get_tuple_accessed_page(((persistent_page){.page = ((void*)page)}), page_size, tpl_def, NULL);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	cy_uint result = find_succeeding_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));

	// NO_TUPLE_FOUND case
	if(result == INVALID_INDEX)
		return NO_TUPLE_FOUND;

	return result;
}

void reverse_sort_order_on_sorted_packed_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def,
									const page_modification_methods* pmm_p
								)
{
	uint32_t count = get_tuple_count_on_page(ppage.page, page_size, &(tpl_def->size_def));
	if(count == 0)
		return ;

	// swap first and last tuples iteratively
	for(uint32_t i = 0; i < count / 2; i++)
		pmm_p->swap_tuples_on_page(pmm_p->context, ppage, page_size, &(tpl_def->size_def), i, count - 1 - i);
}

// on page quick sort algorithm
void sort_and_convert_to_sorted_packed_page(
									persistent_page ppage, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const page_modification_methods* pmm_p
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(ppage.page, page_size, &(tpl_def->size_def));

	// if the page is empty
	if(tuple_count == 0)
		return ;

	tuple_accessed_page tap = get_tuple_accessed_page(ppage, page_size, tpl_def, pmm_p);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, tpl_def, tuple_keys_to_compare, keys_count);
	index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	quick_sort_iai(&iai, 0, tuple_count - 1, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));
}