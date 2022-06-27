#include<bplus_tree_leaf_page_util.h>

#include<sorted_packed_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_index_tuple_functions_util.h>

#include<page_layout.h>
#include<tuple.h>

#include<stdlib.h>

int init_bplus_tree_leaf_page(void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	int inited = init_page(page, bpttd_p->page_size, sizeof_LEAF_PAGE_HEADER(bpttd_p), bpttd_p->record_def);
	if(!inited)
		return 0;
	set_type_of_page(page, bpttd_p->page_size, BPLUS_TREE_LEAF_PAGE);
	set_level_of_bplus_tree_page(page, bpttd_p->page_size, 0);
	set_prev_page_id_of_bplus_tree_leaf_page(page, bpttd_p->NULL_PAGE_ID, bpttd_p);
	set_next_page_id_of_bplus_tree_leaf_page(page, bpttd_p->NULL_PAGE_ID, bpttd_p);
	return 1;
}

void print_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_leaf_page_header(page, bpttd_p);
	print_page(page, bpttd_p->page_size, bpttd_p->record_def);
}

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
static uint32_t calculate_final_tuple_count_of_page_to_be_split(void* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p)
{
	uint32_t tuple_count = get_tuple_count(page1, bpttd_p->page_size, bpttd_p->record_def);

	if(is_fixed_sized_tuple_def(bpttd_p->record_def))
	{
		// if the new tuple is to be inserted after the last tuple in the last leaf page
		if(get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p) == bpttd_p->NULL_PAGE_ID && tuple_to_insert_at == tuple_count)
			return tuple_count;	// i.e. only 1 tuple goes to the new page
		else // else
			return (tuple_count + 1) / 2;	// equal split
	}
	else
	{
		// pre calculate the space that will be occupied by the new tuple
		uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->record_def, tuple_to_insert) + get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->record_def);

		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples(page1, bpttd_p->page_size, bpttd_p->record_def);

		// this is the result number of tuple that should stay on this page
		uint32_t result = 0;

		// if new tuple is to be inserted after the last tuple in the last leaf page => split it such that it is almost full
		if(get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p) == bpttd_p->NULL_PAGE_ID && tuple_to_insert_at == tuple_count)
		{
			uint32_t limit = space_allotted_to_tuples;

			uint32_t space_occupied_until = 0;

			for(uint32_t i = 0; i < tuple_count; i++)
			{
				// if the new tuple is suppossed to be inserted at i then process it first
				if(i == tuple_to_insert_at)
				{
					space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
					if(space_occupied_until <= limit)
						result++;
					if(space_occupied_until >= limit)
						break;
				}

				// process the ith tuple
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples(page1, bpttd_p->page_size, bpttd_p->record_def, i, i);
				space_occupied_until = space_occupied_by_ith_tuple + space_occupied_until;
				if(space_occupied_until <= limit)
					result++;
				if(space_occupied_until >= limit)
					break;
			}

			if(space_occupied_until < limit && tuple_count == tuple_to_insert_at && result == tuple_count)
			{
				// if the new tuple is suppossed to be inserted at the end then append its occupied size to the end
				space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
				if(space_occupied_until <= limit)
					result++;
			}
		}
		else // else => result is the number of tuples that will take the page occupancy just above or equal to 50%
		{
			uint32_t limit = space_allotted_to_tuples / 2;

			uint32_t space_occupied_until = 0;

			for(uint32_t i = 0; i < tuple_count; i++)
			{
				// if the new tuple is suppossed to be inserted at i then process it first
				if(i == tuple_to_insert_at)
				{
					space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
					result++;
					if(space_occupied_until > limit)
						break;
				}

				// process the ith tuple
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples(page1, bpttd_p->page_size, bpttd_p->record_def, i, i);
				space_occupied_until = space_occupied_by_ith_tuple + space_occupied_until;
				result++;
				if(space_occupied_until > limit)
					break;
			}

			if(space_occupied_until <= limit && tuple_count == tuple_to_insert_at && result == tuple_count)
			{
				// if the new tuple is suppossed to be inserted at the end then append its occupied size to the end
				space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
				result++;
			}
		}

		return result;
	}
}

int split_insert_bplus_tree_leaf_page(void* page1, uint64_t page1_id, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, void* output_parent_insert)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_insert_tuple(page1, bpttd_p->page_size, bpttd_p->record_def, tuple_to_insert))
		return 0;

	// we need to make sure that the new_tuple will not be fitting on the page even after a compaction
	// if it does then you should not be calling this function
	uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->record_def, tuple_to_insert) + get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->record_def);
	uint32_t space_available_page1 = get_space_allotted_to_all_tuples(page1, bpttd_p->page_size, bpttd_p->record_def) - get_space_occupied_by_all_tuples(page1, bpttd_p->page_size, bpttd_p->record_def);

	// we fail here because the new tuple can be accomodated in page1, if you had considered compacting the page
	if(space_available_page1 >= space_occupied_by_new_tuple)
		return 0;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page1, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count(page1, bpttd_p->page_size, bpttd_p->record_def);

	// total number of tuples we would be dealing with
	//uint32_t total_tuple_count = page1_tuple_count + 1;

	// lingo for variables page1 => page to be split, page2 => page that will be allocated to handle the split

	// final tuple count of the page that will be split
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page1, tuple_to_insert, tuple_to_insert_at, bpttd_p);

	// final tuple count of the page that will be newly allocated
	//uint32_t final_tuple_count_page2 = total_tuple_count - final_tuple_count_page1;

	// figure out if the new tuple (tuple_to_insert) will go to new page OR should be inserted to the existing page
	int new_tuple_goes_to_page1 = (tuple_to_insert_at < final_tuple_count_page1);

	// number of the tuples of the page1 that will stay in the same page
	uint32_t tuples_stay_in_page1 = final_tuple_count_page1;
	if(new_tuple_goes_to_page1)
		tuples_stay_in_page1--;

	// number of tuples of the page1 that will leave page1 and be moved to page2
	//uint32_t tuples_leave_page1 = page1_tuple_count - tuples_stay_in_page1;

	// allocate a new page
	uint64_t page2_id;
	void* page2 = dam_p->get_new_page_with_write_lock(dam_p->context, &page2_id);

	// return with a split failure if the page2 could not be allocated
	if(page2 == NULL)
		return 0;

	// initialize page2 (as a leaf page)
	init_bplus_tree_leaf_page(page2, bpttd_p);

	// id of the page that is next to page1 (calling this page3)
	uint64_t page3_id = get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p);

	// perform pointer manipulations for the linkedlist of leaf pages
	if(page3_id != bpttd_p->NULL_PAGE_ID)
	{
		// acquire lock on the next page of the page1 (this page will be called page3)
		void* page3 = dam_p->acquire_page_with_writer_lock(dam_p->context, page3_id);

		// return with a split failure if the lock on page3 could not be acquired
		if(page3 == NULL)
		{
			// make sure you free the page that you acquired
			dam_p->release_writer_lock_and_free_page(dam_p->context, page2);
			return 0;
		}

		// perform pointer manipulations to insert page2 between page1 and page3
		set_next_page_id_of_bplus_tree_leaf_page(page1, page2_id, bpttd_p);
		set_prev_page_id_of_bplus_tree_leaf_page(page3, page2_id, bpttd_p);
		set_prev_page_id_of_bplus_tree_leaf_page(page2, page1_id, bpttd_p);
		set_next_page_id_of_bplus_tree_leaf_page(page2, page3_id, bpttd_p);

		// release writer lock on page3, mark it as modified
		dam_p->release_writer_lock_on_page(dam_p->context, page3, 1);
	}
	else
	{
		// perform pointer manipulations to put page2 at the end of this linkedlist after page1
		set_next_page_id_of_bplus_tree_leaf_page(page1, page2_id, bpttd_p);
		set_prev_page_id_of_bplus_tree_leaf_page(page2, page1_id, bpttd_p);
		set_next_page_id_of_bplus_tree_leaf_page(page2, bpttd_p->NULL_PAGE_ID, bpttd_p);
	}

	// while moving tuples, we assume that there will be atleast 1 tuple that will get moved from page1 to page2
	// we made this sure by all the above conditions
	// hence no need to check bounds of start_index and last_index

	// copy all required tuples from the page1 to page2
	insert_all_from_sorted_packed_page(
									page2, page1, bpttd_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->record_def,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// insert the new tuple (tuple_to_insert) to page1 or page2 based on "new_tuple_goes_to_page1", as calculated earlier
	if(new_tuple_goes_to_page1)
	{
		// if can not insert then compact the page first
		if(!can_insert_tuple(page1, bpttd_p->page_size, bpttd_p->record_def, tuple_to_insert))
			run_page_compaction(page1, bpttd_p->page_size, bpttd_p->record_def, 0, 1);

		// insert the tuple_to_insert (the new tuple) at the desired index in the page1
		insert_at_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuple_to_insert, 
									tuple_to_insert_at
								);
	}
	else
	{
		// if can not insert then compact the page first
		if(!can_insert_tuple(page2, bpttd_p->page_size, bpttd_p->record_def, tuple_to_insert))
			run_page_compaction(page2, bpttd_p->page_size, bpttd_p->record_def, 0, 1);

		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									page2, bpttd_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuple_to_insert, 
									tuple_to_insert_at - tuples_stay_in_page1
								);
	}

	// create tuple to be returned, this tuple needs to be inserted into the parent page, after the child_index
	const void* first_tuple_page2 = get_nth_tuple(page2, bpttd_p->page_size, bpttd_p->record_def, 0);

	// intialize parent insert tuple
	init_tuple(bpttd_p->index_def, output_parent_insert);

	// insert key attributes from first_tuple_page2 to output_parent_insert
	for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
		set_element_in_tuple_from_tuple(bpttd_p->index_def, i, output_parent_insert, bpttd_p->record_def, bpttd_p->key_element_ids[i], first_tuple_page2);

	// now insert the pointer to the page2 in this parent tuple
	set_child_page_id_in_index_tuple(output_parent_insert, page2_id, bpttd_p);

	// release lock on the page2, and mark it as modified
	dam_p->release_writer_lock_on_page(dam_p->context, page2, 1);

	// return success
	return 1;
}

int merge_bplus_tree_leaf_pages(void* page1, uint64_t page1_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get the next adjacent page of this page
	uint64_t page2_id = get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p);

	// if it does not exist, return 0 (failure)
	if(page2_id == bpttd_p->NULL_PAGE_ID)
		return 0;

	// acquire lock on the next page, i.e. page2
	void* page2 = dam_p->acquire_page_with_writer_lock(dam_p->context, page2_id);

	// failed acquiring lock on this page, return 0 (failure)
	if(page2 == NULL)
		return 0;

	// check if a merge can be performed
	uint32_t total_space_page1 = get_space_allotted_to_all_tuples(page1, bpttd_p->page_size, bpttd_p->record_def);
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples(page1, bpttd_p->page_size, bpttd_p->record_def);
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples(page2, bpttd_p->page_size, bpttd_p->record_def);

	if(total_space_page1 < space_in_use_page1 + space_in_use_page2)
	{
		// release writer lock on the page, and we did not modify it
		dam_p->release_writer_lock_on_page(dam_p->context, page2, 0);
		return 0;
	}

	// now we can be sure that a merge can be performed on page1 and page2

	// we start by perfoming the pointer manipulation
	// but we need lock on the next page of the page2 to change its previous page pointer
	// we are calling the page next to page2 as page 3

	uint64_t page3_id = get_next_page_id_of_bplus_tree_leaf_page(page2, bpttd_p);

	// page3 does exist and page2 is not the last page
	if(page3_id != bpttd_p->NULL_PAGE_ID)
	{
		// acquire lock on the page3
		void* page3 = dam_p->acquire_page_with_writer_lock(dam_p->context, page3_id);

		// could not acquire lock on page3, so can not perform a merge
		if(page3 == NULL)
		{
			// on error, we release writer lock on page2, suggesting it was not modified
			dam_p->release_writer_lock_on_page(dam_p->context, page2, 0);
			return 0;
		}

		// perform pointer manipulations to remove page2 from the between of page1 and page3
		set_next_page_id_of_bplus_tree_leaf_page(page1, page3_id, bpttd_p);
		set_prev_page_id_of_bplus_tree_leaf_page(page3, page1_id, bpttd_p);

		// release lock on page3, marking it as modified
		dam_p->release_writer_lock_on_page(dam_p->context, page3, 1);
	}
	else // page2 is indeed the last page
	{
		// perform pointer manipulations to make page1 as the last page
		set_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p->NULL_PAGE_ID, bpttd_p);
	}

	// since page2 has been removed from the linkedlist of leaf pages
	// modify page2 pointers (next and prev) to point NULL_PAGE_ID
	// this step is not needed
	set_prev_page_id_of_bplus_tree_leaf_page(page2, bpttd_p->NULL_PAGE_ID, bpttd_p);
	set_next_page_id_of_bplus_tree_leaf_page(page2, bpttd_p->NULL_PAGE_ID, bpttd_p);

	// now, we can safely transfer all tuples from page2 to page1

	// only if there are any tuples to move
	uint32_t tuple_count_page2 = get_tuple_count(page2, bpttd_p->page_size, bpttd_p->record_def);
	if(tuple_count_page2 > 0)
	{
		uint32_t free_space_page1 = get_free_space(page1, bpttd_p->page_size, bpttd_p->record_def);

		// if free space is not enough perform a compaction in advance
		if(free_space_page1 < space_in_use_page2)
			run_page_compaction(page1, bpttd_p->page_size, bpttd_p->record_def, 0, 1);

		// only if there are any tuples to move
		insert_all_from_sorted_packed_page(
									page1, page2, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									0, tuple_count_page2 - 1
								);
	}

	// free page2 and release its lock
	dam_p->release_writer_lock_and_free_page(dam_p->context, page2);

	return 1;
}