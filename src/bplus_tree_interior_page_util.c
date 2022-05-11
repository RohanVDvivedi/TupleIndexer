#include<bplus_tree_interior_page_util.h>

#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

#include<string.h>
#include<stdlib.h>

uint32_t find_child_index_for_key(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	// find preceding equals in the interior pages, by comparing against all index entries
	uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);

	return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
}

uint64_t find_child_page_id_by_child_index(const void* page, uint32_t index, const bplus_tree_tuple_defs* bpttd_p)
{
	// if the index is -1, return the page_id stored in the header
	if(index == -1)
		return get_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p);

	// result child_page_id
	uint64_t child_page_id = 0;

	// tuple of the interior page that is at index
	const void* index_tuple = get_nth_tuple(page, bpttd_p->page_size, bpttd_p->index_def, index);

	// populate child_id using the appropriate element at the end of the tuple
	switch(bpttd_p->page_id_width)
	{
		case 1:
		{
			uint8_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 2:
		{
			uint16_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 4:
		{
			uint32_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 8:
		{
			uint64_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
	}

	return child_page_id;
}

static uint32_t calculate_final_tuple_count_of_page_to_be_split(void* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttds)
{
	uint32_t tuple_count = get_tuple_count(page1, bpttds->page_size, bpttds->index_def);

	if(is_fixed_sized_tuple_def(bpttds->index_def))
	{
		// if it is the last interior page
		if(get_next_page_id_of_bplus_tree_interior_page(page1, bpttds) == bpttds->NULL_PAGE_ID)
			return tuple_count;	// i.e. only 1 tuple goes to the new page
		else // else
			return (tuple_count + 1) / 2;	// equal split
	}
	else
	{
		uint32_t total_tuple_count = tuple_count + 1;
		uint32_t* cumulative_tuple_sizes = malloc(sizeof(uint32_t) * (total_tuple_count + 1));

		cumulative_tuple_sizes[0] = 0;
		for(uint32_t i = 0, k = 1; i < tuple_count; i++)
		{
			// if the new tuple is suppossed to be inserted at i then process it first
			if(i == tuple_to_insert_at)
			{
				uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttds->index_def, tuple_to_insert) + get_additional_space_overhead_per_tuple(bpttds->page_size, bpttds->index_def);
				cumulative_tuple_sizes[k] = space_occupied_by_new_tuple + cumulative_tuple_sizes[k-1];
				k++;
			}

			// process the ith tuple
			uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples(page1, bpttds->page_size, bpttds->index_def, i, i);
			cumulative_tuple_sizes[k] = space_occupied_by_ith_tuple + cumulative_tuple_sizes[k-1];
			k++;
		}

		// now we have the cumulative space requirement of all the tuples
		// i.e. the first n tuples will occupy cumulative_tuple_sizes[n] amount of space

		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples(page1, bpttds->page_size, bpttds->index_def);

		// this is the result number of tuple that should stay on this page
		uint32_t result = 0;

		// if it is the last interior page => split it such that it is almost full
		if(get_next_page_id_of_bplus_tree_interior_page(page1, bpttds) == bpttds->NULL_PAGE_ID)
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

const void* split_insert_interior_page(void* page1, uint64_t page1_id, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_insert_tuple(page1, bpttds->page_size, bpttds->index_def, tuple_to_insert))
		return NULL;

	// we need to make sure that the new_tuple will not be fitting on the page even after a compaction
	// if it does then you should not be calling this function
	uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttds->index_def, tuple_to_insert) + get_additional_space_overhead_per_tuple(bpttds->page_size, bpttds->index_def);
	uint32_t space_available_page1 = get_space_allotted_to_all_tuples(page1, bpttds->page_size, bpttds->index_def) - get_space_occupied_by_all_tuples(page1, bpttds->page_size, bpttds->index_def);

	// we fail here because the new tuple can be accomodated in page1, if you had considered compacting the page
	if(space_available_page1 >= space_occupied_by_new_tuple)
		return NULL;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page1, bpttds->page_size, 
									bpttds->index_def, NULL, bpttds->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count(page1, bpttds->page_size, bpttds->index_def);

	// total number of tuples we would be dealing with
	//uint32_t total_tuple_count = page1_tuple_count + 1;

	// lingo for variables page1 => page to be split, page2 => page that will be allocated to handle the split

	// final tuple count of the page that will be split
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page1, tuple_to_insert, tuple_to_insert_at, bpttds);

	// final tuple count of the page that will be newly allocated
	// this will include the tuple that will be later modified to form the least_keys_page_id and the parent_insert (the tuple that will/should be inserted to the parent page)
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

	// initialize page2 (as an interior page)
	init_page(page2, bpttds->page_size, sizeof_INTERIOR_PAGE_HEADER(bpttds), bpttds->index_def);

	// id of the page that is next to page1 (calling this page3)
	// page3_id == bpttds->NULL_PAGE_ID if page1 is the last page of the list
	uint64_t page3_id = get_next_page_id_of_bplus_tree_interior_page(page1, bpttds);

	// perform pointer manipulations for the singly linkedlist of interior pages of the same level
	set_next_page_id_of_bplus_tree_interior_page(page1, page2_id, bpttds);
	set_next_page_id_of_bplus_tree_interior_page(page2, page3_id, bpttds);

	// while moving tuples, we assume that there will be atleast 1 tuple that will get moved from page1 to page2
	// we made this sure by all the above conditions
	// hence no need to check bounds of start_index and last_index

	// copy all required tuples from the page1 to page2
	insert_all_from_sorted_packed_page(
									page2, page1, bpttds->page_size,
									bpttds->index_def, NULL, bpttds->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page1, bpttds->page_size,
									bpttds->index_def,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// insert the new tuple (tuple_to_insert) to page1 or page2 based on "new_tuple_in_new_page", as calculated earlier
	if(new_tuple_in_new_page)
		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									page2, bpttds->page_size,
									bpttds->index_def, NULL, bpttds->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at - tuples_stay_in_page1
								);
	else
		// insert the tuple_to_insert (the new tuple) at the desired index in the page1
		insert_at_in_sorted_packed_page(
									page1, bpttds->page_size,
									bpttds->index_def, NULL, bpttds->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at
								);

	// now the page2 contains all the other tuples discarded from the page1, (due to the split)
	// The first tuple of the page2 has to be disintegrated as follows
	// first_tuple_page2 =>> key : page_id
	// this page_id of the first tuple needs to become the least_keys_page_id of page2
	// and we then create a parent_insert tuple as =>> key : page2_id
	// we then can delete the first tuple of this page2
	// and then finally we return the parent_insert tuple that can be inserted into parent page

	// create tuple to be returned, this tuple needs to be inserted into the parent page, after the child_index
	const void* first_tuple_page2 = get_nth_tuple(page2, bpttds->page_size, bpttds->index_def, 0);
	uint32_t size_of_first_tuple_page2 = get_tuple_size(bpttds->index_def, first_tuple_page2);

	uint64_t page2_least_keys_page_id = bpttds->NULL_PAGE_ID;

	// get least_keys_page_id for page2 from its first tuple
	switch(bpttds->page_id_width)
	{
		case 1:
		{
			uint8_t p2lkpid;
			copy_element_from_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, first_tuple_page2, &p2lkpid);
			page2_least_keys_page_id = p2lkpid;
			break;
		}
		case 2:
		{
			uint16_t p2lkpid;
			copy_element_from_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, first_tuple_page2, &p2lkpid);
			page2_least_keys_page_id = p2lkpid;
			break;
		}
		case 4:
		{
			uint32_t p2lkpid;
			copy_element_from_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, first_tuple_page2, &p2lkpid);
			page2_least_keys_page_id = p2lkpid;
			break;
		}
		case 8:
		{
			uint64_t p2lkpid;
			copy_element_from_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, first_tuple_page2, &p2lkpid);
			page2_least_keys_page_id = p2lkpid;
			break;
		}
	}

	// set the least_keys_page_id for page2
	set_least_keys_page_id_of_bplus_tree_interior_page(page2, page2_least_keys_page_id, bpttds);

	void* parent_insert = malloc(sizeof(char) * size_of_first_tuple_page2);

	// copy all the contents of the first_tuple_page2 to parent_insert
	memmove(parent_insert, first_tuple_page2, size_of_first_tuple_page2);

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

	// now you may, delete the first tuple of page2
	delete_in_sorted_packed_page(
									page2, bpttds->page_size, 
									bpttds->index_def,
									0
								);

	// release lock on the page2, and mark it as modified
	dam_p->release_writer_lock_on_page(dam_p->context, page2, 1);

	// return parent_insert
	return parent_insert;
}