#include<persistent_page_unaltered_util.h>

int can_append_tuple_on_persistent_page_if_done_resiliently(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple)
{
	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d);
	uint32_t space_required_by_external_tuple = get_space_to_be_occupied_by_tuple_on_persistent_page(page_size, tpl_sz_d, external_tuple);

	// check suggests that there is not enough unused space on page
	if(space_required_by_external_tuple > unused_space_on_page)
		return 0;

	return 1;
}

int can_insert_tuple_on_persistent_page_if_done_resiliently(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d);
	uint32_t space_required_by_external_tuple = get_space_to_be_occupied_by_tuple_on_persistent_page(page_size, tpl_sz_d, external_tuple);

	// check suggests that there is not enough unused space on page
	if(space_required_by_external_tuple > unused_space_on_page)
		return 0;

	return 1;
}

int can_update_tuple_on_persistent_page_if_done_resiliently(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d);

	// get space occupied by existing tuple (tuple at index = index)
	uint32_t space_occupied_by_existing_tuple = get_space_occupied_by_tuples_on_persistent_page(ppage, page_size, tpl_sz_d, index, index);

	// get space required by external tuple on the page
	uint32_t space_required_by_external_tuple = get_space_to_be_occupied_by_tuple_on_persistent_page(page_size, tpl_sz_d, external_tuple);

	// check suggests that there is not enough space on page
	if(space_required_by_external_tuple > unused_space_on_page + space_occupied_by_existing_tuple)
		return 0;

	return 1;
}