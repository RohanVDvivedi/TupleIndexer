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

	while(low < mid)
	{
		const void* tup_mid = get_nth_tuple(page, page_size, key_val_def, mid);
		int compare = compare_tuples(tup_mid, key, key_def);

		(*index) = mid;

		if(compare == 0)
			return 1;
		else if(compare > 0)
			high = mid;
		else
			low = mid;

		mid = (low + high) / 2;
	}

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
	uint16_t index_searched;
	int found = search_in_sorted_packed_page(page, page_size, key_def, key_val_def, key_val, &index_searched);

	const void* tup = get_nth_tuple(page, page_size, key_val_def, index_searched);
	int compare = compare_tuples(tup, key_val, key_def);
	if(compare > 0)
	{
		while(compare > 0)
		{
			tup = get_nth_tuple(page, page_size, key_val_def, index_searched);
			compare = compare_tuples(tup, key_val, key_def);

			index_searched--;
			if(index_searched == 0)
				break;
		}
	}
	else
	{
		while(compare <= 0)
		{
			tup = get_nth_tuple(page, page_size, key_val_def, index_searched);
			compare = compare_tuples(tup, key_val, key_def);

			index_searched++;
			if(index_searched == 0)
				break;
		}
	}

	uint16_t new_index = index_searched;

	// tuple inserted to the end of the page
	insert_tuple(page, page_size, key_val_def, key_val);

	uint16_t tuple_count = get_tuple_count(page);
	uint16_t index = tuple_count - 1;
	while(new_index < index)
	{
		swap_tuples(page, page_size, key_val_def, index - 1, index);
		index--;
	}


	(*index) = new_index;

	return 0;
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

int transfer_sorted_packed_page(
									void* src_page, void* dest_page, uint32_t page_size, 
									const tuple_def* key_val_def, 
									uint16_t start_index, uint16_t end_index
								);