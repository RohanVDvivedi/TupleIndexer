#include<bplus_tree_leaf_page_util.h>

#include<sorted_packed_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_index_tuple_functions_util.h>

#include<page_layout_unaltered.h>
#include<tuple.h>

#include<stdlib.h>

int init_bplus_tree_leaf_page(persistent_page ppage, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p)
{
	int inited = pmm_p->init_page(pmm_p->context, ppage, bpttd_p->page_size, sizeof_LEAF_PAGE_HEADER(bpttd_p), &(bpttd_p->record_def->size_def));
	if(!inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	bplus_tree_leaf_page_header hdr = get_bplus_tree_leaf_page_header(ppage.page, bpttd_p);
	hdr.parent.parent.type = BPLUS_TREE_LEAF_PAGE;
	hdr.parent.level = 0;
	hdr.next_page_id = bpttd_p->NULL_PAGE_ID;
	hdr.prev_page_id = bpttd_p->NULL_PAGE_ID;
	set_bplus_tree_leaf_page_header(ppage, &hdr, bpttd_p, pmm_p);

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
static uint32_t calculate_final_tuple_count_of_page_to_be_split(const void* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p)
{
	uint32_t tuple_count = get_tuple_count_on_page(page1, bpttd_p->page_size, &(bpttd_p->record_def->size_def));

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
		uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->record_def, tuple_to_insert) + get_additional_space_overhead_per_tuple_on_page(bpttd_p->page_size, &(bpttd_p->record_def->size_def));

		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples_on_page(page1, bpttd_p->page_size, &(bpttd_p->record_def->size_def));

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
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples_on_page(page1, bpttd_p->page_size, &(bpttd_p->record_def->size_def), i, i);
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
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples_on_page(page1, bpttd_p->page_size, &(bpttd_p->record_def->size_def), i, i);
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

int must_split_for_insert_bplus_tree_leaf_page(persistent_page page1, const void* tuple_to_insert, const bplus_tree_tuple_defs* bpttd_p)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_append_tuple_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def), tuple_to_insert))
		return 0;

	// we need to make sure that the new_tuple will not be fitting on the page even after a compaction
	// if tuple_to_insert can fit on the page after a compaction, then you should not be splitting the page
	uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->record_def, tuple_to_insert) + get_additional_space_overhead_per_tuple_on_page(bpttd_p->page_size, &(bpttd_p->record_def->size_def));
	uint32_t space_available_page1 = get_space_allotted_to_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def)) - get_space_occupied_by_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));

	// we fail here because the new tuple can be accomodated in page1, if you had considered compacting the page
	if(space_available_page1 >= space_occupied_by_new_tuple)
		return 0;

	return 1;
}

int split_insert_bplus_tree_leaf_page(persistent_page page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, void* output_parent_insert)
{
	// check if a page must split to accomodate the new tuple
	if(!must_split_for_insert_bplus_tree_leaf_page(page1, tuple_to_insert, bpttd_p))
		return 0;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page1.page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));

	// total number of tuples we would be dealing with
	//uint32_t total_tuple_count = page1_tuple_count + 1;

	// lingo for variables page1 => page to be split, page2 => page that will be allocated to handle the split

	// final tuple count of the page that will be split
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page1.page, tuple_to_insert, tuple_to_insert_at, bpttd_p);

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

	// lock the next page of page1, we call it page3
	persistent_page page3 = {};
	page3.page_id = get_next_page_id_of_bplus_tree_leaf_page(page1.page, bpttd_p);
	if(page3.page_id != bpttd_p->NULL_PAGE_ID)
	{
		page3.page = dam_p->acquire_page_with_writer_lock(dam_p->context, page3.page_id);

		// if we could not acquire lock on page3, then fail split_insert
		if(page3.page == NULL)
			return 0;
	}

	// allocate a new page, to place in between page1 and page3, it will accomodate half of the tuples of page1
	persistent_page page2;
	page2.page = dam_p->get_new_page_with_write_lock(dam_p->context, &(page2.page_id));

	// return with a split failure if the page2 could not be allocated
	if(page2.page == NULL)
	{
		// on failure, do not forget to release writer lock on page3
		dam_p->release_writer_lock_on_page(dam_p->context, page3.page, NONE_OPTION);

		return 0;
	}

	// initialize page2 (as a leaf page)
	init_bplus_tree_leaf_page(page2, bpttd_p, pmm_p);

	// link page2 in between page1 and the next page of page1
	{
		// read headers of page1 and page2 inorder to link page2 in between, page1 and page3
		bplus_tree_leaf_page_header page1_hdr = get_bplus_tree_leaf_page_header(page1.page, bpttd_p);
		bplus_tree_leaf_page_header page2_hdr = get_bplus_tree_leaf_page_header(page2.page, bpttd_p);

		// perform pointer manipulations for the linkedlist of leaf pages
		if(page3.page_id != bpttd_p->NULL_PAGE_ID)
		{
			// if page3 exists, then we already have lock on it

			// read page3 header
			bplus_tree_leaf_page_header page3_hdr = get_bplus_tree_leaf_page_header(page3.page, bpttd_p);

			// perform pointer manipulations to insert page2 between page1 and page3
			page1_hdr.next_page_id = page2.page_id;
			page3_hdr.prev_page_id = page2.page_id;
			page2_hdr.prev_page_id = page1.page_id;
			page2_hdr.next_page_id = page3.page_id;

			// set the page3_hdr back onto the page
			set_bplus_tree_leaf_page_header(page3, &page3_hdr, bpttd_p, pmm_p);

			// release writer lock on page3, mark it as modified
			dam_p->release_writer_lock_on_page(dam_p->context, page3.page, WAS_MODIFIED);
		}
		else
		{
			// perform pointer manipulations to put page2 at the end of this linkedlist after page1
			page1_hdr.next_page_id = page2.page_id;
			page2_hdr.prev_page_id = page1.page_id;
			page2_hdr.next_page_id = bpttd_p->NULL_PAGE_ID;
		}

		// page3 not must not be accessed beyond this point, even if it is in your scope

		// set the page1_hdr and page2_hdr back onto the page
		set_bplus_tree_leaf_page_header(page1, &page1_hdr, bpttd_p, pmm_p);
		set_bplus_tree_leaf_page_header(page2, &page2_hdr, bpttd_p, pmm_p);
	}

	// while moving tuples, we assume that there will be atleast 1 tuple that will get moved from page1 to page2
	// we made this sure by all the above conditions
	// hence no need to check bounds of start_index and last_index

	// copy all required tuples from the page1 to page2
	insert_all_from_sorted_packed_page(
									page2, page1, bpttd_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p
								);

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->record_def,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p
								);

	// insert the new tuple (tuple_to_insert) to page1 or page2 based on "new_tuple_goes_to_page1", as calculated earlier
	if(new_tuple_goes_to_page1)
	{
		// insert the tuple_to_insert (the new tuple) at the desired index in the page1
		insert_at_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuple_to_insert, 
									tuple_to_insert_at,
									pmm_p
								);
	}
	else
	{
		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									page2, bpttd_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at - tuples_stay_in_page1,
									pmm_p
								);
	}

	// create tuple to be returned, this tuple needs to be inserted into the parent page, after the child_index
	const void* first_tuple_page2 = get_nth_tuple_on_page(page2.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def), 0);

	// rebuild parent insert tuple using the first record of the page2 and the child_page_id = page2_id
	build_index_entry_from_record_tuple(bpttd_p, first_tuple_page2, page2.page_id, output_parent_insert);

	// release lock on the page2, and mark it as modified
	dam_p->release_writer_lock_on_page(dam_p->context, page2.page, WAS_MODIFIED);

	// return success
	return 1;
}

int can_merge_bplus_tree_leaf_pages(persistent_page page1, persistent_page page2, const bplus_tree_tuple_defs* bpttd_p)
{
	// check if a merge can be performed, by comparing the used space in both the pages
	uint32_t total_space_page1 = get_space_allotted_to_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples_on_page(page2.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));

	if(total_space_page1 < space_in_use_page1 + space_in_use_page2)
		return 0;

	return 1;
}

int merge_bplus_tree_leaf_pages(persistent_page page1, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
{
	// get the next adjacent page of this page
	persistent_page page2;
	page2.page_id = get_next_page_id_of_bplus_tree_leaf_page(page1.page, bpttd_p);

	// if it does not exist, return 0 (failure)
	if(page2.page_id == bpttd_p->NULL_PAGE_ID)
		return 0;

	// acquire lock on the next page, i.e. page2
	page2.page = dam_p->acquire_page_with_writer_lock(dam_p->context, page2.page_id);

	// failed acquiring lock on this page, return 0 (failure)
	if(page2.page == NULL)
		return 0;

	// check if a merge can be performed, and on failure release writer lock on page2, that we just acquired
	if(!can_merge_bplus_tree_leaf_pages(page1, page2, bpttd_p))
	{
		dam_p->release_writer_lock_on_page(dam_p->context, page2.page, NONE_OPTION);
		return 0;
	}

	// now we can be sure that a merge can be performed on page1 and page2

	// we start by perfoming the pointer manipulation
	// but we need lock on the next page of the page2 to change its previous page pointer
	// we are calling the page next to page2 as page 3
	{
		persistent_page page3;
		page3.page_id = get_next_page_id_of_bplus_tree_leaf_page(page2.page, bpttd_p);

		// read headers of page1 and page2 inorder to unlink page2 from the between of page1 and page3
		bplus_tree_leaf_page_header page1_hdr = get_bplus_tree_leaf_page_header(page1.page, bpttd_p);
		bplus_tree_leaf_page_header page2_hdr = get_bplus_tree_leaf_page_header(page2.page, bpttd_p);

		// page3 does exist and page2 is not the last page
		if(page3.page_id != bpttd_p->NULL_PAGE_ID)
		{
			// acquire lock on the page3
			page3.page = dam_p->acquire_page_with_writer_lock(dam_p->context, page3.page_id);

			// could not acquire lock on page3, so can not perform a merge
			if(page3.page == NULL)
			{
				// on error, we release writer lock on page2, suggesting it was not modified
				dam_p->release_writer_lock_on_page(dam_p->context, page2.page, NONE_OPTION);
				return 0;
			}

			// read page3 header
			bplus_tree_leaf_page_header page3_hdr = get_bplus_tree_leaf_page_header(page3.page, bpttd_p);

			// perform pointer manipulations to remove page2 from the between of page1 and page3
			page1_hdr.next_page_id = page3.page_id;
			page3_hdr.prev_page_id = page1.page_id;

			// set the page3_hdr back onto the page
			set_bplus_tree_leaf_page_header(page3, &page3_hdr, bpttd_p, pmm_p);

			// release lock on page3, marking it as modified
			dam_p->release_writer_lock_on_page(dam_p->context, page3.page, WAS_MODIFIED);
		}
		else // page2 is indeed the last page
		{
			// perform pointer manipulations to make page1 as the last page
			page1_hdr.next_page_id = bpttd_p->NULL_PAGE_ID;
		}

		// since page2 has been removed from the linkedlist of leaf pages
		// modify page2 pointers (next and prev) to point NULL_PAGE_ID
		// this step is not needed
		page2_hdr.prev_page_id = bpttd_p->NULL_PAGE_ID;
		page2_hdr.next_page_id = bpttd_p->NULL_PAGE_ID;

		// set the page1_hdr and page2_hdr back onto the page
		set_bplus_tree_leaf_page_header(page1, &page1_hdr, bpttd_p, pmm_p);
		set_bplus_tree_leaf_page_header(page2, &page2_hdr, bpttd_p, pmm_p);
	}

	// now, we can safely transfer all tuples from page2 to page1

	// only if there are any tuples to move
	uint32_t tuple_count_page2 = get_tuple_count_on_page(page2.page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));
	if(tuple_count_page2 > 0)
	{
		// only if there are any tuples to move
		insert_all_from_sorted_packed_page(
									page1, page2, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									0, tuple_count_page2 - 1,
									pmm_p
								);
	}

	// free page2 and release its lock
	dam_p->release_writer_lock_on_page(dam_p->context, page2.page, FREE_PAGE | WAS_MODIFIED);

	return 1;
}