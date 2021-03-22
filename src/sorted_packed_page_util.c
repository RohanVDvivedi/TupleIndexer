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
								);

int transfer_sorted_packed_page(
									void* src_page, void* dest_page, uint32_t page_size, 
									const tuple_def* key_val_def, 
									uint16_t start_index, uint16_t end_index
								);