#include<persistent_page_altered_util.h>

#include<tuple.h>

int append_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error)
{
	// try simply appending, first
	int res = append_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, external_tuple, abort_error);
	if((*abort_error))
		return 0;
	else if(res)
		return 1;

	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d);
	uint32_t space_required_by_external_tuple = ((external_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(tpl_sz_d, external_tuple)) + get_additional_space_overhead_per_tuple_on_persistent_page(page_size, tpl_sz_d);

	// check suggests that there is not enough dpace on page
	if(space_required_by_external_tuple > unused_space_on_page)
		return 0;

	// run_page_compaction, i.e. defragment the page
	run_persistent_page_compaction(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, abort_error);

	if((*abort_error))
		return 0;

	// now try to append, this must succeed
	return append_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, external_tuple, abort_error);
}

int update_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	// try simply updating, first
	int res = update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, external_tuple, abort_error);
	if((*abort_error))
		return 0;
	else if(res)
		return 1;

	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, page_size, tpl_sz_d);

	// get size of existing tuple (at index = index)
	const void* existing_tuple = get_nth_tuple_on_persistent_page(ppage, page_size, tpl_sz_d, index);
	uint32_t existing_tuple_size = ((existing_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(tpl_sz_d, existing_tuple));

	// get size of external_tuple
	uint32_t external_tuple_size = ((external_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(tpl_sz_d, external_tuple));

	// check suggests that there is not enough space on page
	if(external_tuple_size > unused_space_on_page + existing_tuple_size)
		return 0;

	// place tomb_stone for the old tuple at the index
	update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, NULL, abort_error);
	if((*abort_error))
		return 0;

	// run_page_compaction, i.e. defragment the page
	run_persistent_page_compaction(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, abort_error);
	if((*abort_error))
		return 0;

	// then at the end attempt to update the tuple again
	// this time it must succeed
	return update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, external_tuple, abort_error);
}