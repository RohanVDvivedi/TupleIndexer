#include<bplus_tree_leaf_page_util.h>

#include<sorted_packed_page_util.h>
#include<bplus_tree_leaf_page_header.h>

#include<page_layout.h>
#include<tuple.h>

#include<stdlib.h>

uint32_t find_greater_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	return find_succeeding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);
}

uint32_t find_lesser_equals_for_key_bplus_tree_leaf_page(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	return find_preceding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);
}

// this will the tuples that will remain in the page_info after after the complete split operation
static uint32_t calculate_final_tuple_count_of_page_to_be_split(locked_page_info* page_info, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttds)
{
	uint32_t tuple_count = get_tuple_count(page_info->page, bpttds->page_size, bpttds->record_def);

	if(is_fixed_sized_tuple_def(bpttds->record_def))
	{
		// if it is the last leaf page
		if(get_next_page_id_of_bplus_tree_leaf_page(page_info->page, bpttds) == 0)
			return tuple_count;	// i.e. only 1 tuple goes to the new page
		else // else
			return (tuple_count + 1) / 2;	// equal split
	}
	else
	{
		uint32_t total_tuple_count = tuple_count + 1;
		uint32_t* cumulative_tuple_sizes = malloc(sizeof(uint32_t) * (total_tuple_count + 1));

		// space required in addition to its size on the page
		// this is assumed to be fixed
		const void* first_tuple = get_nth_tuple(page_info->page, bpttds->page_size, bpttds->record_def, 0);
		uint32_t per_tuple_over_head = get_space_occupied_by_tuples(page_info->page, bpttds->page_size, bpttds->record_def, 0, 0) - get_tuple_size(bpttds->record_def, first_tuple);

		cumulative_tuple_sizes[0] = 0;
		for(uint32_t i = 0, k = 1; i < tuple_count; i++)
		{
			// if the new tuple is suppossed to be inserted at i then process it first
			if(i == tuple_to_insert_at)
			{
				uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttds->record_def, tuple_to_insert) + per_tuple_over_head;
				cumulative_tuple_sizes[k] = space_occupied_by_new_tuple + cumulative_tuple_sizes[k-1];
				k++;
			}

			// process the ith tuple
			uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples(page_info->page, bpttds->page_size, bpttds->record_def, i, i);
			cumulative_tuple_sizes[k] = space_occupied_by_ith_tuple + cumulative_tuple_sizes[k-1];
			k++;
		}

		// now we have the cumulative space requirement of all the tupl
		// i.e. the first n tuples will occupy cumulative_tuple_sizes[n] amount of space

		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples(page_info->page, bpttds->page_size, bpttds->record_def);

		// this is the result number of tuple that should stay on this page
		uint32_t result = 0;

		// if it is the last leaf page => split it such that it is almost full
		if(get_next_page_id_of_bplus_tree_leaf_page(page_info->page, bpttds) == 0)
		{
			uint32_t limit = space_allotted_to_tuples;

			// perform a binary search to find the cumulative size just above the limit
			// cumulative_tuple_sizes has indices from 0 to total_tuple_count both inclusive
			uint32_t l = 0;
			uint32_t h = total_tuple_count;
			while(l <= h)
			{
				uint32_t m = l + (h - l) / 2;
				if(cumulative_tuple_sizes[m] < limit)
				{
					result = m;
					l = m + 1;
				}
				else if(cumulative_tuple_sizes[m] > limit)
					h = m - 1;
				else
				{
					result = m;
					break;
				}
			}
		}
		else // else => result is the number of tuples that will take the page occupancy just above or equal to 50%
		{
			uint32_t limit = space_allotted_to_tuples / 2;

			// perform a binary search to find the cumulative size just above the limit
			// cumulative_tuple_sizes has indices from 0 to total_tuple_count both inclusive
			uint32_t l = 0;
			uint32_t h = total_tuple_count;
			while(l <= h)
			{
				uint32_t m = l + (h - l) / 2;
				if(cumulative_tuple_sizes[m] < limit)
					l = m + 1;
				else if(cumulative_tuple_sizes[m] > limit)
				{
					result = m;
					h = m - 1;
				}
				else
				{
					result = m;
					break;
				}
			}
		}

		// free cumulative tuple sizes
		free(cumulative_tuple_sizes);

		return result;
	}
}

const void* split_insert_bplus_tree_leaf_page(locked_page_info* page_info, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_insert_tuple(page_info->page, bpttds->page_size, bpttds->record_def, tuple_to_insert))
		return NULL;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page_info->page, bpttds->page_size, 
									bpttds->record_def, bpttds->key_element_ids, bpttds->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count(page_info->page, bpttds->page_size, bpttds->record_def);

	// total number of tuples we would be dealing with
	//uint32_t total_tuple_count = page1_tuple_count + 1;

	// lingo for variables page1 => page to be split, page2 => page that will be allocated to handle the split

	// final tuple count of the page that will be split
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page_info, tuple_to_insert, tuple_to_insert_at, bpttds);

	// final tuple count of the page that will be newly allocated
	//uint32_t final_tuple_count_page2 = total_tuple_count - final_tuple_count_page1;

	// figure out if the new tuple (tuple_to_insert) will go to new page OR should be inserted to the existing page
	int new_tuple_in_new_page = !(tuple_to_insert_at < final_tuple_count_page1);

	// number of the tuples of the page1 that will stay in the same page
	uint32_t tuples_stay_in_page1 = final_tuple_count_page1;
	if(!new_tuple_in_new_page)
		tuples_stay_in_page1--;

	// number of tuples of the page1 that will leave page1 and be moved to page2
	//uint32_t tuples_leave_page1 = page1_tuple_count - tuples_stay_in_page1;

	// allocate a new page
	uint64_t page2_id;
	void* page2 = dam_p->get_new_page_with_write_lock(dam_p->context, &page2_id);

	// return with a split failure if the page2 could not be allocated
	if(page2 == NULL)
		return NULL;

	// initialize page2 (as a leaf page)
	init_page(page2, bpttds->page_size, sizeof_LEAF_PAGE_HEADER(bpttds), bpttds->record_def);

	// id of the page that is next to page1 (calling this page3)
	uint64_t page3_id = get_next_page_id_of_bplus_tree_leaf_page(page_info->page, bpttds);

	// perform pointer manipulations for the linkedlist of leaf pages
	if(page3_id != 0)
	{
		// acquire lock on the next page of the page1 (this page will be called page3)
		void* page3 = dam_p->acquire_page_with_writer_lock(dam_p->context, page3_id);

		// return with a split failure if the lock on page3 could not be acquired
		if(page3 == NULL)
		{
			// make sure you free the page that you acquired
			dam_p->release_writer_lock_and_free_page(dam_p->context, page2);
			return NULL;
		}

		// perform pointer manipulations to insert page2 between page1 and page3
		set_next_page_id_of_bplus_tree_leaf_page(page_info->page, page2_id, bpttds);
		set_prev_page_id_of_bplus_tree_leaf_page(page3, page2_id, bpttds);
		set_prev_page_id_of_bplus_tree_leaf_page(page2, page_info->page_id, bpttds);
		set_next_page_id_of_bplus_tree_leaf_page(page2, page3_id, bpttds);

		// release writer lock on page3
		dam_p->release_writer_lock_on_page(dam_p->context, page3);
	}
	else
	{
		// perform pointer manipulations to put page2 at the end of this linkedlist after page1
		set_next_page_id_of_bplus_tree_leaf_page(page_info->page, page2_id, bpttds);
		set_prev_page_id_of_bplus_tree_leaf_page(page2, page_info->page_id, bpttds);
		set_next_page_id_of_bplus_tree_leaf_page(page2, 0, bpttds);
	}

	// copy all required tuples from the page1 to page2
	insert_all_from_sorted_packed_page(
									page2, page_info->page, bpttds->page_size,
									bpttds->record_def, bpttds->key_element_ids, bpttds->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page_info->page, bpttds->page_size,
									bpttds->record_def,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// insert the new tuple (tuple_to_insert) to page1 or page2 based on "new_tuple_in_new_page", as calculated earlier
	if(new_tuple_in_new_page)
		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									page2, bpttds->page_size,
									bpttds->record_def, bpttds->key_element_ids, bpttds->key_element_count,
									tuple_to_insert, 
									tuple_to_insert_at - tuples_stay_in_page1
								);
	else
		// insert the tuple_to_insert (the new tuple) at the desired index in the page1
		insert_at_in_sorted_packed_page(
									page_info->page, bpttds->page_size,
									bpttds->record_def, bpttds->key_element_ids, bpttds->key_element_count,
									tuple_to_insert, 
									tuple_to_insert_at
								);

	// create tuple to be returned, this tuple needs to be inserted into the parent page, after the child_index
	const void* first_tuple_page2 = get_nth_tuple(page2, bpttds->page_size, bpttds->record_def, 0);
	uint32_t size_of_first_tuple_page2 = get_tuple_size(bpttds->record_def, first_tuple_page2);

	void* parent_insert = malloc(sizeof(char) * (size_of_first_tuple_page2 + bpttds->page_id_width));

	// insert key attributes from first_tuple_page2 to parent_insert
	for(uint32_t i = 0; i < bpttds->key_element_count; i++)
		set_element_in_tuple_from_tuple(bpttds->index_def, i, parent_insert, bpttds->record_def, bpttds->key_element_ids[i], first_tuple_page2);

	// now insert the pointer to the page2 in this parent tuple
	switch(bpttds->page_id_width)
	{
		case 1:
		{
			uint8_t p2id = page2_id;
			set_element_in_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &p2id, 1);
			break;
		}
		case 2:
		{
			uint16_t p2id = page2_id;
			set_element_in_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &p2id, 2);
			break;
		}
		case 4:
		{
			uint32_t p2id = page2_id;
			set_element_in_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &p2id, 4);
			break;
		}
		case 8:
		{
			uint64_t p2id = page2_id;
			set_element_in_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &p2id, 8);
			break;
		}
	}

	// release lock on the page2
	dam_p->release_writer_lock_on_page(dam_p->context, page2);

	// return parent_insert
	return parent_insert;
}

int merge_bplus_tree_leaf_pages(locked_page_info* page_info1, locked_page_info* page_info2, bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p)
{

}