#include<sorted_packed_page_util.h>

#include<tuple.h>
#include<page_layout.h>

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
	// the actual page pointer
	void* page;

	// the size of the page
	uint32_t page_size;

	// tuple_def to be used with the page
	const tuple_def* tpl_def;
};

static const void* get_tuple_from_tuple_accessed_page(const void* ds_p, cy_uint index)
{
	const tuple_accessed_page* tap_p = ds_p;
	return get_nth_tuple_on_page(tap_p->page, tap_p->page_size, &(tap_p->tpl_def->size_def), index);
}

// set functionality will not be provided

static int swap_tuples_in_tuple_accessed_page(void* ds_p, cy_uint i1, cy_uint i2)
{
	tuple_accessed_page* tap_p = ds_p;
	return swap_tuples_on_page(tap_p->page, tap_p->page_size, &(tap_p->tpl_def->size_def), i1, i2);
}

static cy_uint get_tuple_count_for_tuple_accessed_page(const void* ds_p)
{
	const tuple_accessed_page* tap_p = ds_p;
	return get_tuple_count_on_page(tap_p->page, tap_p->page_size, &(tap_p->tpl_def->size_def));
}

#define get_tuple_accessed_page(page_v, page_size_v, tpl_def_v) ((const tuple_accessed_page){.page = page_v, .page_size = page_size_v, .tpl_def = tpl_def_v})

index_accessed_interface get_index_accessed_interface_for_sorted_packed_page(tuple_accessed_page* tap_p)
{
	return (index_accessed_interface) {
		.ds_p = tap_p,
		.get_element = get_tuple_from_tuple_accessed_page,
		.set_element = NULL, // set element functionality will not be provided
		.swap_elements = swap_tuples_in_tuple_accessed_page,
		.get_element_count = get_tuple_count_for_tuple_accessed_page
	};
}

// ---------------- UTILTY CODE FOR SORTED PACKED PAGE END ---------------------------------------------------

// the 0 <= index <= tuple_count
// internal function to insert a tuple at a specified index
// this function does not check that the sort order is maintained
static int insert_at_in_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def,
									const void* tuple, 
									uint32_t index
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the index is not valid we fail the insertion
	if( !(0 <= index && index <= tuple_count) )
		return 0;

	// insert tuple to the end of the page
	if(!append_tuple_on_page(page, page_size, &(tpl_def->size_def), tuple))
		return 0;

	// insert succeedded, so tuple_count incremented
	tuple_count++;

	// curr_index that the tuple is at
	uint32_t curr_index = tuple_count - 1;

	// swap until the new tuple is not at index
	while(index != curr_index)
	{
		swap_tuples_on_page(page, page_size, &(tpl_def->size_def), curr_index - 1, curr_index);
		curr_index--;
	}

	return 1;
}

int insert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t* index
								)
{
	// search for a viable index for the new tuple to insert
	uint32_t new_index = find_insertion_point_in_sorted_packed_page(page, page_size, tpl_def, tuple_keys_to_compare, keys_count, tuple);

	// this is the final index for the newly inserted element
	if(index != NULL)
		(*index) = new_index;

	// insert tuple to the page at the desired index
	return insert_at_in_page(page, page_size, tpl_def, tuple, new_index);
}

int insert_at_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the index is not valid we fail the insertion
	if( !(0 <= index && index <= tuple_count) )
		return 0;

	// in the below conditions we ensure that the tuple order is maintained

	// the tuple compares greater than the tuple at index, we fail
	if(tuple_count > 0 && index < tuple_count)
	{
		const void* ith_tuple = get_nth_tuple_on_page(page, page_size, &(tpl_def->size_def), index);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, ith_tuple, tpl_def, tuple_keys_to_compare, keys_count) > 0)
			return 0;
	}

	// the tuple compares lesser than the tuple at (index - 1), we fail
	if(tuple_count > 0 && index > 0)
	{
		const void* i_1_th_tuple = get_nth_tuple_on_page(page, page_size, &(tpl_def->size_def), index - 1);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, i_1_th_tuple, tpl_def, tuple_keys_to_compare, keys_count) < 0)
			return 0;
	}

	// insert tuple to the page at the desired index
	return insert_at_in_page(page, page_size, tpl_def, tuple, index);
}

int update_resiliently_at_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index
								)
{
	uint32_t tuple_count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));

	// if the index is not valid we fail the update
	if( !(0 <= index && index < tuple_count) )
		return 0;

	// in the below conditions we ensure that the tuple order is maintained

	// the tuple compares greater than the tuple at (index + 1), we fail
	if(tuple_count > 0 && index != tuple_count - 1)
	{
		const void* i_1_th_tuple = get_nth_tuple_on_page(page, page_size, &(tpl_def->size_def), index + 1);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, i_1_th_tuple, tpl_def, tuple_keys_to_compare, keys_count) > 0)
			return 0;
	}

	// the tuple compares lesser than the tuple at (index - 1), we fail
	if(tuple_count > 0 && index > 0)
	{
		const void* i_1_th_tuple = get_nth_tuple_on_page(page, page_size, &(tpl_def->size_def), index - 1);
		if( compare_tuples(tuple, tpl_def, tuple_keys_to_compare, i_1_th_tuple, tpl_def, tuple_keys_to_compare, keys_count) < 0)
			return 0;
	}

	int update_successfull = update_tuple_on_page(page, page_size, &(tpl_def->size_def), index, tuple);

	// if simple update was successfull (without defragmenting and discarding tombstones)
	if(update_successfull)
		return update_successfull;

	// get free space on page after it gets defragmented
	// here we assume that the page passed to this functions has no tombstones as should be the case with sorted_packed_page
	uint32_t free_space_after_defragmentation = get_free_space_on_page(page, page_size, &(tpl_def->size_def)) + get_fragmentation_space_on_page(page, page_size, &(tpl_def->size_def));

	// get size of existing tuple (at index = index)
	const void* existing_tuple = get_nth_tuple_on_page(page, page_size, &(tpl_def->size_def), index);
	uint32_t existing_tuple_size = get_tuple_size(tpl_def, existing_tuple);

	// if discarding the existing tuple can make enough room for the new tuple then
	if(free_space_after_defragmentation + existing_tuple_size >= get_tuple_size(tpl_def, tuple))
	{
		// place tomb_stone for the old tuple at the index
		update_tuple_on_page(page, page_size, &(tpl_def->size_def), index, NULL);

		// defragment the page
		run_page_compaction(page, page_size, &(tpl_def->size_def));

		// then at the end attempt to update the tuple again
		// this time it must succeed
		update_successfull = update_tuple_on_page(page, page_size, &(tpl_def->size_def), index, tuple);
	}

	return update_successfull;
}

int delete_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def,
									uint32_t index
								)
{
	return discard_tuple_on_page(page, page_size, &(tpl_def->size_def), index);
}

int delete_all_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, 
									uint32_t start_index, uint32_t end_index
								)
{
	uint32_t count = get_tuple_count_on_page(page, page_size, &(tpl_def->size_def));
	if(count == 0 || start_index > end_index || end_index >= count)
		return 0;

	// if the user wants to discard all tuple, do it quickly using
	if(start_index == 0 && end_index == count - 1)
		return discard_all_tuples_on_page(page, page_size, &(tpl_def->size_def));

	for(uint32_t i = start_index; i <= end_index; i++)
		discard_tuple_on_page(page, page_size, &(tpl_def->size_def), start_index); // a discarded tuple does not leave a slot, hence always discarding at start_index

	return 1;
}

static uint32_t append_tuples_from_page(void* page_dest, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* page_src, uint32_t start_index, uint32_t last_index)
{
	uint32_t appended_count = 0;
	for(uint32_t i = start_index; i <= last_index; i++)
	{
		const void* tup = get_nth_tuple_on_page(page_src, page_size, tpl_sz_d, i);
		if(tup == NULL)
			continue;

		int res = append_tuple_on_page(page_dest, page_size, tpl_sz_d, tup);
		if(res == 0)
			break;

		appended_count++;
	}

	return appended_count;
}

uint32_t insert_all_from_sorted_packed_page(
									void* page_dest, const void* page_src, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									uint32_t start_index, uint32_t end_index
								)
{
	uint32_t src_count = get_tuple_count_on_page(page_src, page_size, &(tpl_def->size_def));
	if(src_count == 0 || start_index > end_index || end_index >= src_count)
		return 0;

	// if the dest page is empty, insert all no comparisons needed
	uint32_t dest_count = get_tuple_count_on_page(page_dest, page_size, &(tpl_def->size_def));
	if(dest_count == 0)
		return append_tuples_from_page(page_dest, page_size, &(tpl_def->size_def), page_src, start_index, end_index);

	// compare the last tuple of the dest page and first tuple of the src page
	const void* last_tuple_dest = get_nth_tuple_on_page(page_dest, page_size, &(tpl_def->size_def), dest_count - 1);
	const void* first_tuple_src = get_nth_tuple_on_page(page_src, page_size, &(tpl_def->size_def), 0);

	// if they are in order then perform a direct copy
	int compare_last_first = compare_tuples(last_tuple_dest, tpl_def, tuple_keys_to_compare, first_tuple_src, tpl_def, tuple_keys_to_compare, keys_count);
	if(compare_last_first <= 0)
		return append_tuples_from_page(page_dest, page_size, &(tpl_def->size_def), page_src, start_index, end_index);

	uint32_t inserted_count = 0;

	// insert using stupstom api from start_index to end_index
	for(uint32_t index = start_index; index <= end_index; index++)
	{
		const void* tup = get_nth_tuple_on_page(page_src, page_size, &(tpl_def->size_def), index);
		if(tup == NULL)
			continue;

		int res = insert_to_sorted_packed_page(page_dest, page_size, tpl_def, tuple_keys_to_compare, keys_count, tup, NULL);
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

	tuple_accessed_page tap = get_tuple_accessed_page(((void*)page), page_size, tpl_def);
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

	tuple_accessed_page tap = get_tuple_accessed_page(((void*)page), page_size, tpl_def);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	return binary_search_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context), FIRST_OCCURENCE);
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

	tuple_accessed_page tap = get_tuple_accessed_page(((void*)page), page_size, tpl_def);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	return binary_search_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context), LAST_OCCURENCE);
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

	tuple_accessed_page tap = get_tuple_accessed_page(((void*)page), page_size, tpl_def);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	return find_preceding_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));
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

	tuple_accessed_page tap = get_tuple_accessed_page(((void*)page), page_size, tpl_def);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	return find_preceding_or_equals_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));
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

	tuple_accessed_page tap = get_tuple_accessed_page(((void*)page), page_size, tpl_def);
	const tuple_on_page_compare_context topcc = get_tuple_on_page_compare_context(tpl_def, tuple_keys_to_compare, key_def, key_elements_to_compare, keys_count);
	const index_accessed_interface iai = get_index_accessed_interface_for_sorted_packed_page(&tap);

	return find_succeeding_or_equals_in_sorted_iai(&iai, 0, tuple_count - 1, key, &contexted_comparator(&topcc, compare_tuples_using_comparator_context));
}

uint32_t find_succeeding_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									const void* key, const tuple_def* key_def, const uint32_t* key_elements_to_compare
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return NO_TUPLE_FOUND;

	// if the provided tuple is lesser than the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, tuple_keys_to_compare, key, key_def, key_elements_to_compare, keys_count) > 0)
		return 0;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NO_TUPLE_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, tuple_keys_to_compare, key, key_def, key_elements_to_compare, keys_count);

		if(compare > 0)
		{
			found_index = mid;
			high = mid - 1;
		}
		else if(compare <= 0)
			low = mid + 1;
	}

	return found_index;
}

void reverse_sort_order_on_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return ;

	// swap first and last tuples iteratively
	for(uint32_t i = 0; i < count / 2; i++)
		swap_tuples(page, page_size, tpl_def, i, count - 1 - i);
}

// on page quick sort algorithm
static void sort_and_convert_to_sorted_packed_page_recursively(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count,
									uint32_t first, uint32_t last
								)
{
	if(first >= last)
		return;

	const void* pivot = get_nth_tuple(page, page_size, tpl_def, last);

	uint32_t j = first;
	for(uint32_t i = first; i <= last; i++)
	{
		const void* ith_tuple = get_nth_tuple(page, page_size, tpl_def, i);
		if(compare_tuples(ith_tuple, tpl_def, tuple_keys_to_compare, pivot, tpl_def, tuple_keys_to_compare, keys_count) <= 0)
			swap_tuples(page, page_size, tpl_def, i, j++);
	}

	uint32_t pivot_index = j - 1;

	if(pivot_index > first)
		sort_and_convert_to_sorted_packed_page_recursively(page, page_size, tpl_def, tuple_keys_to_compare, keys_count, first, pivot_index - 1);
	if(pivot_index < last)
		sort_and_convert_to_sorted_packed_page_recursively(page, page_size, tpl_def, tuple_keys_to_compare, keys_count, pivot_index + 1, last);
}

void sort_and_convert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, const uint32_t* tuple_keys_to_compare, uint32_t keys_count
								)
{
	// remove tomb stones of deleted tuples
	run_page_compaction(page, page_size, tpl_def, 1, 0);

	// if tuple count is lesser or equal to 1, then there is nothing to do
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count <= 1)
		return ;

	// use quick sort to sort in place
	sort_and_convert_to_sorted_packed_page_recursively(page, page_size, tpl_def, tuple_keys_to_compare, keys_count, 0, count - 1);
}