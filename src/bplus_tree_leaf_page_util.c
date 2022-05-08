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
static uint32_t calculate_final_tuple_count_of_page_to_be_split(locked_page_info* page_info, const void* tuple_to_insert, const bplus_tree_tuple_defs* bpttds)
{
	if(is_fixed_sized_tuple_def(bpttds->record_def))
	{
	}
	else
	{
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
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page_info, tuple_to_insert, bpttds);

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
	// TODO

	// release lock on the page2
	dam_p->release_writer_lock_on_page(dam_p->context, page2);

	// return parent_insert
	return parent_insert;
}

int merge_bplus_tree_leaf_pages(locked_page_info* page_info1, locked_page_info* page_info2, bplus_tree_tuple_defs* bpttds, data_access_methods* dam_p)
{

}