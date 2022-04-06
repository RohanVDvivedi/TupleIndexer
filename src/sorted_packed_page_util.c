#include<sorted_packed_page_util.h>

#include<tuple.h>
#include<page_layout.h>

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
	uint32_t tuple_count = get_tuple_count(page, page_size, tpl_def);

	// if the index is not valid we fail the insertion
	if( !(0 <= index && index <= tuple_count) )
		return 0;

	// insert tuple to the end of the page
	if(!insert_tuple(page, page_size, tpl_def, tuple))
		return 0;

	// insert succeedded, so tuple_count incremented
	tuple_count++;

	// curr_index that the tuple is at
	uint32_t curr_index = tuple_count - 1;

	// swap until the new tuple is not at index
	while(index != curr_index)
	{
		swap_tuples(page, page_size, tpl_def, curr_index - 1, curr_index);
		curr_index--;
	}

	return 1;
}

int insert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t* index
								)
{
	// search for a viable index for the new tuple to insert
	uint32_t new_index = find_insertion_point_in_sorted_packed_page(page, page_size, tpl_def, keys_to_compare, keys_count, tuple);

	// this is the final index for the newly inserted element
	if(index != NULL)
		(*index) = new_index;

	// insert tuple to the page at the desired index
	return insert_at_in_page(page, page_size, tpl_def, tuple, new_index);
}

int insert_at_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index
								)
{
	uint32_t tuple_count = get_tuple_count(page, page_size, tpl_def);

	// if the index is not valid we fail the insertion
	if( !(0 <= index && index <= tuple_count) )
		return 0;

	// in the below conditions we ensure that the tuple order is maintained

	// the tuple compares greater than the tuple at index, we fail
	if(tuple_count > 0 && index < tuple_count)
	{
		const void* ith_tuple = get_nth_tuple(page, page_size, tpl_def, index);
		if( compare_tuples(tuple, tpl_def, keys_to_compare, ith_tuple, tpl_def, keys_to_compare, keys_count) > 0)
			return 0;
	}

	// the tuple compares lesser than the tuple at (index - 1), we fail
	if(tuple_count > 0 && index > 0)
	{
		const void* i_1_th_tuple = get_nth_tuple(page, page_size, tpl_def, index - 1);
		if( compare_tuples(tuple, tpl_def, keys_to_compare, i_1_th_tuple, tpl_def, keys_to_compare, keys_count) < 0)
			return 0;
	}

	// insert tuple to the page at the desired index
	return insert_at_in_page(page, page_size, tpl_def, tuple, index);
}

int update_resiliently_at_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple, 
									uint32_t index
								)
{
	uint32_t tuple_count = get_tuple_count(page, page_size, tpl_def);

	// if the index is not valid we fail the update
	if( !(0 <= index && index < tuple_count) )
		return 0;

	// in the below conditions we ensure that the tuple order is maintained

	// the tuple compares greater than the tuple at (index + 1), we fail
	if(tuple_count > 0 && index != tuple_count - 1)
	{
		const void* i_1_th_tuple = get_nth_tuple(page, page_size, tpl_def, index + 1);
		if( compare_tuples(tuple, tpl_def, keys_to_compare, i_1_th_tuple, tpl_def, keys_to_compare, keys_count) > 0)
			return 0;
	}

	// the tuple compares lesser than the tuple at (index - 1), we fail
	if(tuple_count > 0 && index > 0)
	{
		const void* i_1_th_tuple = get_nth_tuple(page, page_size, tpl_def, index - 1);
		if( compare_tuples(tuple, tpl_def, keys_to_compare, i_1_th_tuple, tpl_def, keys_to_compare, keys_count) < 0)
			return 0;
	}

	int update_successfull = update_tuple(page, page_size, tpl_def, index, tuple);

	// if simple update was successfull (without defragmenting and discarding tombstones)
	if(update_successfull)
		return update_successfull;

	// get free space on page after it gets defragmented
	// here we assume that the page passed to this functions has no tombstones as should be the case with sorted_packed_page
	uint32_t free_space_after_defragmentation = get_free_space(page, page_size, tpl_def) + get_fragmentation_space(page, page_size, tpl_def);

	// get size of existing tuple (at index = index)
	const void* existing_tuple = get_nth_tuple(page, page_size, tpl_def, index);
	uint32_t existing_tuple_size = get_tuple_size(tpl_def, existing_tuple);

	// if discarding the existing tuple can make enough room for the new tuple then
	if(free_space_after_defragmentation + existing_tuple_size >= get_tuple_size(tpl_def, tuple))
	{
		// delete the old tuple at the index
		delete_tuple(page, page_size, tpl_def, index);

		// defragment the page (do not discard the tomb stone of the tuple we just deleted above)
		run_page_compaction(page, page_size, tpl_def, 0, 1);

		// then at the end attempt to update the tuple again
		// this time it must succeed
		update_successfull = update_tuple(page, page_size, tpl_def, index, tuple);
	}

	return update_successfull;
}

int delete_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def,
									uint32_t index
								)
{
	if(!delete_tuple(page, page_size, tpl_def, index))
		return 0;

	run_page_compaction(page, page_size, tpl_def, 1, 0);

	return 1;
}

int delete_all_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, 
									uint32_t start_index, uint32_t end_index
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0 || start_index > end_index || end_index >= count)
		return 0;

	for(uint32_t i = start_index; i <= end_index; i++)
		delete_tuple(page, page_size, tpl_def, i);

	run_page_compaction(page, page_size, tpl_def, 1, 0);

	return 1;
}

uint32_t insert_all_from_sorted_packed_page(
									void* page_dest, const void* page_src, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									uint32_t start_index, uint32_t end_index
								)
{
	uint32_t src_count = get_tuple_count(page_src, page_size, tpl_def);
	if(src_count == 0 || start_index > end_index || end_index >= src_count)
		return 0;

	// if the dest page is empty, insert all no comparisons needed
	uint32_t dest_count = get_tuple_count(page_dest, page_size, tpl_def);
	if(dest_count == 0)
		return insert_tuples_from_page(page_dest, page_size, tpl_def, page_src, start_index, end_index);

	// compare the last tuple of the dest page and first tuple of the src page
	const void* last_tuple_dest = get_nth_tuple(page_dest, page_size, tpl_def, dest_count - 1);
	const void* first_tuple_src = get_nth_tuple(page_src, page_size, tpl_def, 0);

	// if they are in order then perform a direct copy
	int compare_last_first = compare_tuples(last_tuple_dest, tpl_def, keys_to_compare, first_tuple_src, tpl_def, keys_to_compare, keys_count);
	if(compare_last_first <= 0)
		return insert_tuples_from_page(page_dest, page_size, tpl_def, page_src, start_index, end_index);

	uint32_t inserted_count = 0;

	// insert using stupstom api from start_index to end_index
	for(uint32_t index = start_index; index <= end_index; index++)
	{
		const void* tup = get_nth_tuple(page_src, page_size, tpl_def, index);
		if(insert_to_sorted_packed_page(page_dest, page_size, tpl_def, keys_to_compare, keys_count, tup, NULL))
			inserted_count++;
		else
			break;
	}

	return inserted_count;
}

uint32_t find_insertion_point_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple
									)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return 0;

	// if the provided tuple is lesser than the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count) > 0)
		return 0;

	uint32_t insertion_index = NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		if(mid == count)
		{
			insertion_index = count;
			break;
		}

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count);

		if(compare > 0)
		{
			insertion_index = mid;
			high = mid - 1;
		}
		else
			low = mid + 1;
	}

	return insertion_index;
}

uint32_t find_first_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return NOT_FOUND;

	// if the provided tuple is lesser than the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count) > 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count);

		if(compare > 0)
			high = mid - 1;
		else if(compare < 0)
			low = mid + 1;
		else
		{
			found_index = mid;
			if(mid == 0)
				break;
			high = mid - 1;
		}
	}

	return found_index;
}

uint32_t find_last_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return NOT_FOUND;

	// if the provided tuple is lesser than the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count) > 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count);

		if(compare > 0)
			high = mid - 1;
		else if(compare < 0)
			low = mid + 1;
		else
		{
			found_index = mid;
			low = mid + 1;
		}
	}

	return found_index;
}

uint32_t find_preceding_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return NOT_FOUND;

	// if the provided tuple is lesser than or equal to the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count) >= 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count);

		if(compare >= 0)
			high = mid - 1;
		else if(compare < 0)
		{
			found_index = mid;
			low = mid + 1;
		}
	}

	return found_index;
}

uint32_t find_preceding_equals_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return NOT_FOUND;

	// if the provided tuple is lesser than or equal to the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count) > 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count);

		if(compare > 0)
			high = mid - 1;
		else if(compare <= 0)
		{
			found_index = mid;
			low = mid + 1;
		}
	}

	return found_index;
}

uint32_t find_succeeding_equals_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return NOT_FOUND;

	// if the provided tuple is lesser than the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count) > 0)
		return 0;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count);

		if(compare >= 0)
		{
			found_index = mid;
			if(mid == 0)
				break;
			high = mid - 1;
		}
		else if(compare < 0)
			low = mid + 1;
	}

	return found_index;
}

uint32_t find_succeeding_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
									const void* tuple
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return NOT_FOUND;

	// if the provided tuple is lesser than the first tuple
	const void* tup_first = get_nth_tuple(page, page_size, tpl_def, 0);
	if(compare_tuples(tup_first, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count) > 0)
		return 0;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tpl_def, keys_to_compare, tuple, tpl_def, keys_to_compare, keys_count);

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
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count,
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
		if(compare_tuples(ith_tuple, tpl_def, keys_to_compare, pivot, tpl_def, keys_to_compare, keys_count) <= 0)
			swap_tuples(page, page_size, tpl_def, i, j++);
	}

	uint32_t pivot_index = j - 1;

	if(pivot_index > first)
		sort_and_convert_to_sorted_packed_page_recursively(page, page_size, tpl_def, keys_to_compare, keys_count, first, pivot_index - 1);
	if(pivot_index < last)
		sort_and_convert_to_sorted_packed_page_recursively(page, page_size, tpl_def, keys_to_compare, keys_count, pivot_index + 1, last);
}

void sort_and_convert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count
								)
{
	// remove tomb stones of deleted tuples
	run_page_compaction(page, page_size, tpl_def, 1, 0);

	// if tuple count is lesser or equal to 1, then there is nothing to do
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count <= 1)
		return ;

	// use quick sort to sort in place
	sort_and_convert_to_sorted_packed_page_recursively(page, page_size, tpl_def, keys_to_compare, keys_count, 0, count - 1);
}