#include<page_modification_methods.h>

#include<tuple.h>
#include<page_layout_unaltered.h>

int append_tuple_on_page_resiliently(const page_modification_methods* pmm_p, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple)
{
	// try simply appending, first
	if(pmm_p->append_tuple_on_page(pmm_p->context, ppage, page_size, tpl_sz_d, external_tuple))
		return 1;

	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_page(ppage.page, page_size, tpl_sz_d) - get_space_occupied_by_all_tuples_on_page(ppage.page, page_size, tpl_sz_d);
	uint32_t space_required_by_external_tuple = ((external_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(tpl_sz_d, external_tuple)) + get_additional_space_overhead_per_tuple_on_page(page_size, tpl_sz_d);

	// check suggests that there is not enough dpace on page
	if(space_required_by_external_tuple > unused_space_on_page)
		return 0;

	// run_page_compaction, i.e. defragment the page
	pmm_p->run_page_compaction(pmm_p->context, ppage, page_size, tpl_sz_d);

	// now try to append, this must succeed
	return pmm_p->append_tuple_on_page(pmm_p->context, ppage, page_size, tpl_sz_d, external_tuple);
}

int update_tuple_on_page_resiliently(const page_modification_methods* pmm_p, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	// try simply updating, first
	if(pmm_p->update_tuple_on_page(pmm_p->context, ppage, page_size, tpl_sz_d, index, external_tuple))
		return 1;

	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_page(ppage.page, page_size, tpl_sz_d) - get_space_occupied_by_all_tuples_on_page(ppage.page, page_size, tpl_sz_d);

	// get size of existing tuple (at index = index)
	const void* existing_tuple = get_nth_tuple_on_page(ppage.page, page_size, tpl_sz_d, index);
	uint32_t existing_tuple_size = ((existing_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(tpl_sz_d, existing_tuple));

	// get size of external_tuple
	uint32_t external_tuple_size = ((external_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(tpl_sz_d, external_tuple));

	// check suggests that there is not enough space on page
	if(external_tuple_size > unused_space_on_page + existing_tuple_size)
		return 0;

	// place tomb_stone for the old tuple at the index
	pmm_p->update_tuple_on_page(pmm_p->context, ppage, page_size, tpl_sz_d, index, NULL);

	// run_page_compaction, i.e. defragment the page
	pmm_p->run_page_compaction(pmm_p->context, ppage, page_size, tpl_sz_d);

	// then at the end attempt to update the tuple again
	// this time it must succeed
	return pmm_p->update_tuple_on_page(pmm_p->context, ppage, page_size, tpl_sz_d, index, external_tuple);
}