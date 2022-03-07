#include<sorted_packed_page_util.h>

#include<tuple.h>
#include<page_layout.h>

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

	// insert tuple to the end of the page
	if(!insert_tuple(page, page_size, tpl_def, tuple))
		return 0;

	// swap until the new tuple is not at new_index
	uint32_t tuple_count = get_tuple_count(page, page_size, tpl_def);
	uint32_t temp_index = tuple_count - 1;
	while(new_index < temp_index)
	{
		swap_tuples(page, page_size, tpl_def, temp_index - 1, temp_index);
		temp_index--;
	}

	return 1;
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
	int compare_last_first = compare_tuples(last_tuple_dest, first_tuple_src, tpl_def, keys_count, keys_to_compare);
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
	if(compare_tuples(tup_first, tuple, tpl_def, keys_count, keys_to_compare) > 0)
		return 0;

	uint32_t insertion_index = NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		if(mid == count)
			break;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tuple, tpl_def, keys_count, keys_to_compare);

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
	if(compare_tuples(tup_first, tuple, tpl_def, keys_count, keys_to_compare) > 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tuple, tpl_def, keys_count, keys_to_compare);

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
	if(compare_tuples(tup_first, tuple, tpl_def, keys_count, keys_to_compare) > 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tuple, tpl_def, keys_count, keys_to_compare);

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
	if(compare_tuples(tup_first, tuple, tpl_def, keys_count, keys_to_compare) >= 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tuple, tpl_def, keys_count, keys_to_compare);

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
	if(compare_tuples(tup_first, tuple, tpl_def, keys_count, keys_to_compare) > 0)
		return NOT_FOUND;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tuple, tpl_def, keys_count, keys_to_compare);

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
	if(compare_tuples(tup_first, tuple, tpl_def, keys_count, keys_to_compare) > 0)
		return 0;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tuple, tpl_def, keys_count, keys_to_compare);

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
	if(compare_tuples(tup_first, tuple, tpl_def, keys_count, keys_to_compare) > 0)
		return 0;

	uint32_t low = 0;
	uint32_t high = count - 1;

	uint32_t found_index = NOT_FOUND;

	while(low <= high)
	{
		uint32_t mid = low + (high - low) / 2;

		const void* tup_mid = get_nth_tuple(page, page_size, tpl_def, mid);
		int compare = compare_tuples(tup_mid, tuple, tpl_def, keys_count, keys_to_compare);

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
									const tuple_def* tpl_def, uint32_t* keys_to_compare, uint32_t keys_count
								)
{
	uint32_t count = get_tuple_count(page, page_size, tpl_def);
	if(count == 0)
		return ;

	// swap first and last tuples iteratively
	for(uint32_t i = 0; i < count / 2; i++)
		swap_tuples(page, page_size, tpl_def, i, count - 1 - i);
}