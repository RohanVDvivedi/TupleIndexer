#include<sorted_packed_page_util.h>

#include<tuple.h>
#include<page_layout.h>

int search_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* key_val_def, 
									const void* key, 
									uint16_t* index
								)
{
	uint16_t count = get_tuple_count(page);
	if(count == 0)
		return 0;

	uint16_t low = 0;
	uint16_t high = count - 1;

	uint16_t mid = (low + high) / 2;
	(*index) = mid;

	const void* tup_mid;
	int compare;

	while(low < high)
	{
		mid = (low + high) / 2;
		(*index) = mid;

		tup_mid = get_nth_tuple(page, page_size, key_val_def, mid);
		compare = compare_tuples(tup_mid, key, key_def);

		if(compare > 0)
		{
			if(mid == 0)
				break;
			high = mid - 1;
		}
		else if(compare < 0)
		{
			if(mid == 65535)
				break;
			low = mid + 1;
		}
		else
			return 1;
	}

	mid = (low + high) / 2;
	(*index) = mid;

	tup_mid = get_nth_tuple(page, page_size, key_val_def, mid);
	compare = compare_tuples(tup_mid, key, key_def);

	if(compare == 0)
		return 1;

	return 0;
}

int insert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* key_val_def, 
									const void* key_val, 
									uint16_t* index
								)
{
	if(!can_accomodate_tuple_insert(page, page_size, key_val_def, key_val))
		return 0;

	// handle edge case when number of elements in the page are 0
	if(get_tuple_count(page) == 0)
	{
		insert_tuple(page, page_size, key_val_def, key_val);
		(*index) = 0;
		return 1;
	}

	// search for a viable index that is closer to that one, which we are planning to insert
	uint16_t index_searched = 0;
	search_in_sorted_packed_page(page, page_size, key_def, key_val_def, key_val, &index_searched);

	const void* tup = get_nth_tuple(page, page_size, key_val_def, index_searched);
	int compare = compare_tuples(tup, key_val, key_def);
	if(compare > 0)
	{
		while(compare > 0)
		{
			if(index_searched == 0)
				break;

			index_searched--;

			tup = get_nth_tuple(page, page_size, key_val_def, index_searched);
			compare = compare_tuples(tup, key_val, key_def);
		}
		if(compare <= 0)
			index_searched++;
	}
	else
	{
		uint16_t tuple_count = get_tuple_count(page);
		while(compare <= 0)
		{
			index_searched++;

			if(index_searched == tuple_count)
				break;

			tup = get_nth_tuple(page, page_size, key_val_def, index_searched);
			compare = compare_tuples(tup, key_val, key_def);
		}
	}

	uint16_t new_index = index_searched;

	// tuple inserted to the end of the page
	insert_tuple(page, page_size, key_val_def, key_val);

	uint16_t tuple_count = get_tuple_count(page);
	uint16_t temp_index = tuple_count - 1;	// current temporary index of the inserted tuple
	while(new_index < temp_index)
	{
		swap_tuples(page, page_size, key_val_def, temp_index - 1, temp_index);
		temp_index--;
	}


	(*index) = new_index;

	return 1;
}

int delete_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* key_val_def,
									uint16_t index
								)
{
	uint16_t count = get_tuple_count(page);
	if(count == 0 || index >= count)
		return 0;

	int deleted = delete_tuple(page, page_size, key_val_def, index);

	if(deleted == 0)
		return 0;

	// move the tombstone to the end
	while(index < count - 1)
	{
		swap_tuples(page, page_size, key_val_def, index, index + 1);
		index++;
	}

	return 1;
}

int delete_all_in_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* key_val_def, 
									uint16_t start_index, uint16_t end_index
								)
{
	uint16_t count = get_tuple_count(page);
	if(count == 0 || start_index > end_index || end_index >= count)
		return 0;

	for(uint16_t i = start_index; i <= end_index; i++)
		delete_tuple(page, page_size, key_val_def, i);

	// move tombstones to the end
	for(uint16_t i = start_index, j = end_index + 1; j < count; i++, j++)
		swap_tuples(page, page_size, key_val_def, i, j);

	return 1;
}

uint16_t insert_all_from_sorted_packed_page(
									void* page_dest, const void* page_src, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* key_val_def, 
									uint16_t start_index, uint16_t end_index
								)
{
	uint16_t count = get_tuple_count(page_src);
	if(count == 0 || start_index > end_index || end_index >= count)
		return 0;

	// if the dest page does not have cany tuples then we can perform a direct insert and that will be in order
	// i.e. no comparisons needed
	if(get_tuple_count(page_dest) == 0)
		return insert_tuples_from_page(page_dest, page_size, key_val_def, page_src, start_index, end_index);

	// compare the last tuple of the dest page and first tuple of the src page
	const void* last_tuple_dest = get_nth_tuple(page_dest, page_size, key_val_def, get_tuple_count(page_dest) - 1);
	const void* first_tuple_src = get_nth_tuple(page_src, page_size, key_val_def, 0);

	// if they are in order then perform a direct copy
	int compare_last_first = compare_tuples(last_tuple_dest, first_tuple_src, key_def);
	if(compare_last_first <= 0)
		return insert_tuples_from_page(page_dest, page_size, key_val_def, page_src, start_index, end_index);

	uint16_t inserted_count = 0;

	// insert using stupstom api from start_index to end_index
	for(uint16_t index = start_index; index <= end_index; index++)
	{
		const void* tup = get_nth_tuple(page_src, page_size, key_val_def, index);
		uint16_t inserted_index;
		int inserted = insert_to_sorted_packed_page(page_dest, page_size, key_def, key_val_def, tup, &inserted_index);
		if(inserted)
			inserted_count++;
		else
			break;
	}

	return inserted_count;
}