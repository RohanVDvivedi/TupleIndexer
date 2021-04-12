#include<bplus_tree_interior_page_util.h>

#include<tuple.h>
#include<page_layout.h>

#include<sorted_packed_page_util.h>
#include<storage_capacity_page_util.h>

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
	return *(get_element_from_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, index_tuple).UINT_4);
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

int32_t find_in_interior_page(const void* page, uint32_t page_size, const void* like_key, const bplus_tree_tuple_defs* bpttds)
{
	uint16_t tuple_count = get_tuple_count(page);

	// if there are no index tuples we may just jump to the ALL_LEAST_VALUES_REF reference
	if(tuple_count == 0)
		return -1;

	const void* first_index_tuple = get_nth_tuple(page, page_size, bpttds->index_def, 0);
	int compare = compare_tuples(like_key, first_index_tuple, bpttds->key_def);

	// i.e. get the ALL_LEAST_VALUES_REF reference
	if(compare < 0)
		return -1;

	uint16_t index_searched = 0;
	int found = search_in_sorted_packed_page(page, page_size, bpttds->key_def, bpttds->index_def, like_key, &index_searched);

	if(found)
		return index_searched;

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

int can_accomodate_new_index_entry_without_split_interior_page(const void* page, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	switch(get_page_layout_type(bpttds->index_def))
	{
		case FIXED_ARRAY_PAGE_LAYOUT :
		{
			uint32_t free_space = get_free_space_in_page(page, page_size, bpttds->index_def);
			uint32_t new_tuple_size = bpttds->index_def->size + get_additional_space_occupied_per_tuple(page_size, bpttds->index_def);
			return free_space >= new_tuple_size;
		}
		default :
		{
			// since each record/index tuple is lesser than half full
			// we assume that a page lesser than or half full will surely accomodate any new the tuple to this page
			return is_page_lesser_than_or_equal_to_half_full(page, page_size, bpttds->index_def);
		}
	}
}

static int can_split_interior_page(void* page_to_be_split, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// can not split if the page is less than half full
	if(is_page_lesser_than_or_equal_to_half_full(page_to_be_split, page_size, bpttds->index_def))
		return 0;

	return 1;
}

void* split_insert_interior_page(void* page_to_be_split, const void* new_index_entry, void* new_page, uint32_t new_page_id, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// if can not split the page
	if(!can_split_interior_page(page_to_be_split, page_size, bpttds))
		return NULL;

	// index_entry_count before the split
	uint32_t index_entry_count = get_index_entry_count_in_interior_page(page_to_be_split);

	// initialize temp page
	uint32_t temp_page_size = 2 * page_size;
	void* temp_page = malloc(temp_page_size);

	{
		// init as any other interior page
		init_interior_page(temp_page, temp_page_size, bpttds);

		// insert all records to the temp_page (including the new record)
		for(uint16_t i = 0; i < index_entry_count; i++)
		{
			const void* tuple_to_move = get_nth_tuple(page_to_be_split, page_size, bpttds->index_def, i);
			insert_tuple(temp_page, temp_page_size, bpttds->index_def, tuple_to_move);
		}
		insert_to_sorted_packed_page(temp_page, temp_page_size, bpttds->key_def, bpttds->index_def, new_index_entry, NULL);

		// and reset both the pages, to insert the split tuples

		// init new interior page
		init_interior_page(new_page, page_size, bpttds);

		// delete all in page_to_be_split
		delete_all_tuples(page_to_be_split, page_size, bpttds->index_def);
	}

	// this is the new index entry count, considering that the new tuple is inserted to this one temp page, instead of 2 split pages
	index_entry_count = get_index_entry_count_in_interior_page(temp_page);

	// the return value
	void* parent_insert = NULL;

	// actual move operation, to move tuples to either of page_to_be_split or to new_page
	{
		uint16_t tuple_to_insert = 0;

		while(is_page_lesser_than_half_full(page_to_be_split, page_size, bpttds->index_def))
		{
			const void* tuple_to_move = get_nth_tuple(temp_page, temp_page_size, bpttds->index_def, tuple_to_insert);
			int inserted = insert_tuple(page_to_be_split, page_size, bpttds->index_def, tuple_to_move);
			if(inserted)
				tuple_to_insert++;
			else
				break;
		}

		// get this next to be inserted index_entry
		const void* page_entry_to_insert = get_index_entry_from_interior_page(temp_page, temp_page_size, tuple_to_insert, bpttds);
		uint32_t page_entry_to_insert_key_size = get_tuple_size(bpttds->key_def, page_entry_to_insert);

		// now prepare the parent record
		parent_insert = malloc(page_entry_to_insert_key_size + 4);
		memmove(parent_insert, page_entry_to_insert, page_entry_to_insert_key_size);
		copy_element_to_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &new_page_id);

		// use this same entry to set the ALL_LEAST_REF of the new_page
		uint32_t all_least_values_ref_new_page = get_index_page_id_from_interior_page(temp_page, temp_page_size, tuple_to_insert, bpttds);
		set_index_page_id_in_interior_page(new_page, page_size, -1, bpttds, all_least_values_ref_new_page);

		tuple_to_insert++;

		for(; tuple_to_insert < index_entry_count; tuple_to_insert++)
		{
			const void* tuple_to_move = get_nth_tuple(temp_page, temp_page_size, bpttds->index_def, tuple_to_insert);
			insert_tuple(new_page, page_size, bpttds->index_def, tuple_to_move);
		}
	}

	free(temp_page);

	return parent_insert;
}

int is_surely_more_than_half_full_even_after_delete_at_index(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds)
{
	uint32_t tuple_size = get_space_occupied_by_tuples(page, page_size, bpttds->index_def, index, index);
	uint32_t free_space = get_free_space_in_page(page, page_size, bpttds->index_def);
	uint32_t new_best_case_free_space = free_space + tuple_size;
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, bpttds->index_def);
	uint32_t new_best_case_used_space = allotted_space - new_best_case_free_space;
	return new_best_case_used_space > (allotted_space / 2);
}

static int can_merge_interior_pages(void* page, const void* parent_index_record, void* sibling_page_to_be_merged, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// atleast one of the two pages must be lesser than half full
	if(is_page_more_than_or_equal_to_half_full(page, page_size, bpttds->index_def) &&
		is_page_more_than_or_equal_to_half_full(sibling_page_to_be_merged, page_size, bpttds->index_def))
		return 0;

	// calculate the size of the parent index record, that we have to insert to the merged page
	uint32_t parent_index_record_size = get_tuple_size(bpttds->key_def, parent_index_record) + 4
									+ get_additional_space_occupied_per_tuple(page_size, bpttds->index_def);

	// check if all the tuples in the sibling page + the parent_index_record can be inserted into the page
	if(get_free_space_in_page(page, page_size, bpttds->index_def) < (parent_index_record_size + get_space_occupied_by_all_tuples(sibling_page_to_be_merged, page_size, bpttds->index_def)))
		return 0;

	return 1;
}

int merge_interior_pages(void* page, const void* parent_index_record, void* sibling_page_to_be_merged, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// check if merge is possible
	if(!can_merge_interior_pages(page, parent_index_record, sibling_page_to_be_merged, page_size, bpttds))
		return 0;

	// calculate the size of the key of the parent index record
	uint32_t parent_index_record_key_size = get_tuple_size(bpttds->key_def, parent_index_record);

	{// insert parent index entry
		uint32_t all_least_ref_sibling_page = get_index_page_id_from_interior_page(sibling_page_to_be_merged, page_size, -1, bpttds);

		// create an index entry corresponding to the parent index record
		void* parent_index_to_be_inserted = malloc(parent_index_record_key_size + 4);
		memmove(parent_index_to_be_inserted, parent_index_record, parent_index_record_key_size);
		copy_element_to_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_index_to_be_inserted, &all_least_ref_sibling_page);

		// insert the newly created parent_index_entry and insert it to the page
		insert_to_sorted_packed_page(page, page_size, bpttds->key_def, bpttds->index_def, parent_index_to_be_inserted, NULL);
		free(parent_index_to_be_inserted);

		// it is redundant operation to set index page_id in a page that we are already going to free
		set_index_page_id_in_interior_page(sibling_page_to_be_merged, page_size, -1, bpttds, NULL_PAGE_REF);
	}

	{// insert sibling index entries
		// tuples in the sibling page, that we need to copy to the page
		uint16_t tuples_to_be_merged = get_tuple_count(sibling_page_to_be_merged);

		// insert sibling page index entries to the page
		insert_all_from_sorted_packed_page(page, sibling_page_to_be_merged, page_size, bpttds->key_def, bpttds->index_def, 0, tuples_to_be_merged - 1);

		// delete all tuuples in the sibling page (that is to be free, hence a redundant operation)
		delete_all_in_sorted_packed_page(sibling_page_to_be_merged, page_size, bpttds->index_def, 0, tuples_to_be_merged - 1);
	}

	return 1;
}