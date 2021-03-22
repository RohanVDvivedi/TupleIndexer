#include<sorted_packed_page_util.h>

int search_in_sorted_packed_page(
									const void* page, uint32_t page_size, 
									const tuple_def* key_df, const tuple_def* key_val_def, 
									const void* key, 
									uint16_t* index
								)
{
	uint16_t low = 0;
	uint16_t high = get_tuple_count(page);

	uint16_t mid = (low + high) / 2;

	while(low < high)
	{
		const void* tup_mid = get_nth_tuple();
		int compare = compare_tuples(tup_mid, key, key_df);

		(*index) = mid;

		if(compare == 0)
			break;
		else if(compare > 0)
			high = mid;
		else
			low = mid;

		mid = (low + high) / 2;
	}

	// index points to the data that is just equal to the key
	if(compare == 0)
		return 1;

	// index points to the data that is just lesser than the key
	return 0;
}

int insert_to_sorted_packed_page(
									void* page, uint32_t page_size, 
									const tuple_def* key_def, const tuple_def* key_val_def, 
									const void* tuple, 
									uint16_t* index
								);

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