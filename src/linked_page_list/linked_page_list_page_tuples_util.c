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

int can_merge_linked_page_list_pages(const persistent_page* page1, const persistent_page* page2, const linked_page_list_tuple_defs* lpltd_p)
{
	// make sure that the next of page1 is page2 and the prev of page2 is page1
	if(!is_next_of_in_linked_page_list(page1, page2, lpltd_p)
	|| !is_prev_of_in_linked_page_list(page2, page1, lpltd_p))
		return 0;

	// check if a merge can be performed, by comparing the used space in both the pages
	// NOTE :: it is assumed here that all pages of linked_page_list have identical format, and hence will have same total_space for tuples
	uint32_t total_space_page1_OR_page2 = get_space_allotted_to_all_tuples_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def));
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples_on_persistent_page(page1, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def));
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples_on_persistent_page(page2, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def));

	if(total_space_page1_OR_page2 < space_in_use_page1 + space_in_use_page2)
		return 0;

	return 1;
}