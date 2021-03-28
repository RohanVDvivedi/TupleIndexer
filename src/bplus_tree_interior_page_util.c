#include<bplus_tree_interior_page_util.h>

#include<tuple.h>
#include<page_layout.h>

#include<sorted_packed_page_util.h>

#include<string.h>
#include<stdlib.h>

int is_interior_page(const void* page)
{
	return get_page_type(page) == INTERIOR_PAGE_TYPE;
}

int init_interior_page(void* page, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	return init_page(page, page_size, INTERIOR_PAGE_TYPE, 1, bpttds->index_def);
}

uint16_t get_index_entry_count_in_interior_page(const void* page)
{
	return get_tuple_count(page);
}

uint32_t get_index_page_id_from_interior_page(const void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds)
{
	if(index == -1)
		return get_reference_page_id(page, ALL_LEAST_VALUES_REF);

	// if index is out of bounds
	if(index >= get_tuple_count(page) || index < -1)
		return 0;

	const void* index_tuple = get_nth_tuple(page, page_size, bpttds->index_def, index);
	return *(get_element(bpttds->index_def, bpttds->index_def->element_count - 1, index_tuple).UINT_4);
}

int set_index_page_id_in_interior_page(void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds, uint32_t page_id)
{
	if(index == -1)
		return set_reference_page_id(page, ALL_LEAST_VALUES_REF, page_id);

	// if index is out of bounds
	if(index >= get_tuple_count(page) || index < -1)
		return 0;

	void* index_tuple = (void*) get_nth_tuple(page, page_size, bpttds->index_def, index);
	copy_element_to_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, index_tuple, &page_id);

	return 1;
}

const void* get_index_entry_from_interior_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds)
{
	return get_nth_tuple(page, page_size, bpttds->index_def, index);
}

int32_t find_in_interior_page(const void* page, uint32_t page_size, const void* like_key, int pick_last_match, const bplus_tree_tuple_defs* bpttds)
{
	const void* first_index_tuple = get_nth_tuple(page, page_size, bpttds->index_def, 0);
	int compare = compare_tuples(like_key, first_index_tuple, bpttds->key_def);

	// i.e. get the ALL_LEAST_VALUES_REF reference
	if(compare < 0)
		return -1;

	uint16_t index_searched = 0;
	int found = search_in_sorted_packed_page(page, page_size, bpttds->key_def, bpttds->index_def, like_key, &index_searched);

	uint16_t tuple_count = get_tuple_count(page);

	if(found)
	{
		if(pick_last_match)
		{
			// loop over index_searched incrementingly while compare == 0
			for(uint16_t i = index_searched; i < tuple_count; i++)
			{
				const void* i_tuple = get_nth_tuple(page, page_size, bpttds->index_def, i);
				compare = compare_tuples(like_key, i_tuple, bpttds->key_def);
				
				if(compare == 0)
				{
					index_searched = i;
					i++;
				}
				else
					break;
			}
			return index_searched;
		}
		else
		{
			// loop over index_searched decrementingly while compare == 0
			uint16_t i = index_searched;
			while(1)
			{
				const void* i_tuple = get_nth_tuple(page, page_size, bpttds->index_def, i);
				compare = compare_tuples(like_key, i_tuple, bpttds->key_def);
				
				if(compare == 0)
					index_searched = i;
				else
					break;

				if(i == 0)
					break;
				i--;
			}
			return index_searched;
		}
	}

	const void* index_searched_tuple =  get_nth_tuple(page, page_size, bpttds->index_def, index_searched);
	compare = compare_tuples(like_key, index_searched_tuple, bpttds->key_def);

	if(compare > 0)
	{
		for(uint16_t i = index_searched; i < tuple_count; i++)
		{
			const void* i_tuple = get_nth_tuple(page, page_size, bpttds->index_def, i);
			compare = compare_tuples(like_key, i_tuple, bpttds->key_def);

			if(compare > 0)
				index_searched = i;
			else
				break;
		}
	}
	else if(compare < 0)
	{
		uint16_t i = index_searched;
		while(1)
		{
			const void* i_tuple = get_nth_tuple(page, page_size, bpttds->index_def, i);
			compare = compare_tuples(like_key, i_tuple, bpttds->key_def);

			if(compare > 0)
			{
				index_searched = i;
				break;
			}

			if(i == 0)
				break;
			else
				i--;
		}
	}
	
	return index_searched;
}

const void* split_interior_page(void* page_to_be_split, void* new_page, uint32_t new_page_id, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	uint32_t index_entry_count = get_index_entry_count_in_interior_page(page_to_be_split);

	// can not split if the index entry count is lesser than 3
	if(index_entry_count < 3)
		return NULL;

	// after the split
	uint16_t new_index_entry_count = index_entry_count / 2;

	// endex entry that should be moved to the parent page
	uint16_t index_entry_parent = new_index_entry_count;

	uint16_t index_entries_to_move_start = new_index_entry_count + 1;
	uint16_t index_entries_to_move_last = index_entry_count;

	// init a new interior page
	init_interior_page(new_page, page_size, bpttds);

	// insert index entries to new page
	insert_all_from_sorted_packed_page(new_page, page_to_be_split, page_size, bpttds->key_def, bpttds->index_def, index_entries_to_move_start, index_entries_to_move_last);

	// delete the copied endex entries in the page that is to be split
	delete_all_in_sorted_packed_page(page_to_be_split, page_size, bpttds->index_def, index_entries_to_move_start, index_entries_to_move_last);

	// set ALL_LEAST_PAGE_REF for the new_page
	uint32_t all_least_values_ref_new_page = get_index_page_id_from_interior_page(page_to_be_split, page_size, index_entry_parent, bpttds);
	set_index_page_id_in_interior_page(new_page, page_size, -1, bpttds, all_least_values_ref_new_page);

	// get first record in the new page
	const void* page_entry_to_insert = get_index_entry_from_interior_page(page_to_be_split, page_size, index_entry_parent, bpttds);
	uint32_t page_entry_to_insert_key_size = get_tuple_size(bpttds->key_def, page_entry_to_insert);

	// generate the tuple with index_def, that you need to insert in the parent page
	uint32_t parent_insert_tuple_size = page_entry_to_insert_key_size + 4;
	void* parent_insert = malloc(parent_insert_tuple_size);

	// set the key and new_page_id for the new index entry that we need to return, to be inserted to the parent page
	memmove(parent_insert, page_entry_to_insert, page_entry_to_insert_key_size);
	copy_element_to_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &new_page_id);

	// delete the index_entry that is to be inserted to the parent
	delete_in_sorted_packed_page(page_to_be_split, page_size, bpttds->index_def, index_entry_parent);

	return parent_insert;
}