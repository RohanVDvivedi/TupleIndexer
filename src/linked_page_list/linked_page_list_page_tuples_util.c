#include<linked_page_list_page_tuples_util.h>

#include<linked_page_list_page_header.h>
#include<linked_page_list_node_util.h>

#include<persistent_page_functions.h>
//#include<virtual_unsplitted_persistent_page.h>

int must_split_for_insert_linked_page_list_page(const persistent_page* page1, const void* tuple_to_insert, const linked_page_list_tuple_defs* lpltd_p)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_append_tuple_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), tuple_to_insert))
		return 0;

	// we need to make sure that the new_tuple will not be fitting on the page even after a compaction
	// if tuple_to_insert can fit on the page after a compaction, then you should not be splitting the page
	uint32_t space_to_be_occupied_by_new_tuple = get_space_to_be_occupied_by_tuple_on_persistent_page(lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), tuple_to_insert);
	uint32_t space_available_page1 = get_space_allotted_to_all_tuples_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def)) - get_space_occupied_by_all_tuples_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def));

	// we fail here because the new tuple can be accomodated in page1, if you had considered compacting the page
	if(space_available_page1 >= space_to_be_occupied_by_new_tuple)
		return 0;

	return 1;
}

persistent_page split_insert_bplus_tree_interior_page(persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, int split_type, int split_organization, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// check if a page must split to accomodate the new tuple
	if(!must_split_for_insert_linked_page_list_page(page1, tuple_to_insert, lpltd_p))
		return get_NULL_persistent_page(pam_p);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));

	// total number of tuples we would be dealing with
	uint32_t total_tuple_count = page1_tuple_count + 1;

	// final tuple count in the upper half split
	uint32_t final_tuple_count_in_upper_half_split = calculate_final_tuple_count_in_upper_half_split_of_page_to_be_split(page1, tuple_to_insert, tuple_to_insert_at, bpttd_p);

	// final tuple count in the lower_half split
	uint32_t final_tuple_count_in_lower_half_split = total_tuple_count - final_tuple_count_in_upper_half_split;

	// figure out if the new tuple (tuple_to_insert) will go to upper_half or the lower_half
	int new_tuple_goes_to_upper_half = (tuple_to_insert_at < final_tuple_count_in_upper_half_split);

	// number of tuples of page1 that will become part of upper_half split
	uint32_t final_upper_half_tuples_from_page1 = final_tuple_count_in_upper_half_split;
	if(new_tuple_goes_to_upper_half)
		final_upper_half_tuples_from_page1--;

	// number of tuples of page1 that will become part of lower_half split
	uint32_t final_lower_half_tuples_from_page1 = page1_tuple_count - final_upper_half_tuples_from_page1;

	// TODO
}

int can_merge_linked_page_list_pages(const persistent_page* page1, const persistent_page* page2, const linked_page_list_tuple_defs* lpltd_p)
{
	// make sure that the next of page1 is page2 and the prev of page2 is page1
	if(!is_next_of_in_linked_page_list(page1, page2, lpltd_p)
	|| !is_prev_of_in_linked_page_list(page2, page1, lpltd_p))
		return 0;

	// check if a merge can be performed, by comparing the used space in both the pages
	// NOTE :: it is assumed here that all pages of linked_page_list have identical format, and hence will have same total_space for tuples
	uint32_t total_space_page1_OR_page2 = get_space_to_be_allotted_to_all_tuples_on_persistent_page(sizeof_LINKED_PAGE_LIST_PAGE_HEADER(lpltd_p), lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def));
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def));
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples_on_persistent_page(page2, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def));

	if(total_space_page1_OR_page2 < space_in_use_page1 + space_in_use_page2)
		return 0;

	return 1;
}

int merge_linked_page_list_pages(persistent_page* page1, persistent_page* page2, int merge_into, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// ensure that we can merge
	if(!can_merge_linked_page_list_pages(page1, page2, lpltd_p))
		return 0;

	// perform merge by copying tuples, in the direction of the merge
	switch(merge_into)
	{
		case MERGE_INTO_PAGE1 :
		{
			// append all tuples of page2 to the end of page1, starting with the first tuple
			for(uint32_t i2 = 0; i2 < get_tuple_count_on_persistent_page(page2, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def)); i2++)
			{
				const void* t2 = get_nth_tuple_on_persistent_page(page2, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), i2);
				append_tuple_on_persistent_page_resiliently(pmm_p, transaction_id, page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), t2, abort_error);
				if(*abort_error)
					return 0;
			}
			break;
		}
		case MERGE_INTO_PAGE2 :
		{
			// insert all tuples of page1 to the index 0 of page2, starting with the last tuple
			for(uint32_t i1 = get_tuple_count_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def)) - 1; i1 != UINT32_MAX; i1--)
			{
				const void* t1 = get_nth_tuple_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), i1);
				insert_tuple_on_persistent_page_resiliently(pmm_p, transaction_id, page2, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), 0, t1, abort_error);
				if(*abort_error)
					return 0;
			}
			break;
		}
	}

	return 1;
}