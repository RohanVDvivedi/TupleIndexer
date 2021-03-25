#include<bplus_tree_interior_page_util.h>

#include<tuple.h>
#include<page_layout.h>

#include<sorted_packed_page_util.h>

int is_interior_page(const void* page)
{
	return get_page_type(page) == INTERIOR_PAGE_TYPE;
}

uint16_t get_index_entry_count_in_interior_page(const void* page)
{
	return get_tuple_count(page);
}

uint32_t get_index_page_id_from_interior_page(const void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds)
{
	if(index == -1)
		return get_reference_page_id(page, ALL_LEAST_REF);

	// if index is out of bounds
	if(index >= get_tuple_count(page) || index < -1)
		return 0;

	const void* index_tuple = get_nth_tuple(page, page_size, bpttds->index_def, index);
	return *(get_element(bpttds->index_def, bpttds->index_def->element_count - 1, index_tuple).UINT_4);
}

int set_index_page_id_in_interior_page(void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds, uint32_t page_id)
{
	if(index == -1)
		return set_reference_page_id(page, ALL_LEAST_REF, page_id);

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

int32_t find_in_interior_page(const void* page, uint32_t page_size, const void* like_key, const bplus_tree_tuple_defs* bpttds)
{
	const void* first_index_tuple = get_nth_tuple(page, page_size, bpttds->index_def, 0);
	int compare = compare_tuples(like_key, first_index_tuple, bpttds->key_def);

	// i.e. get the ALL_LEAST_REF reference
	if(compare < 0)
		return -1;

	uint16_t index_searched = 0;
	int found = search_in_sorted_packed_page(page, page_size, bpttds->key_def, bpttds->index_def, like_key, &index_searched);

	if(found)
		return index_searched;

	uint16_t tuple_count = get_tuple_count(page);

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