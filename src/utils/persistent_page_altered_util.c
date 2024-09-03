#include<persistent_page_altered_util.h>

int append_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error)
{
	// if can not append even resiliently, we fail right here
	if(!can_append_tuple_on_persistent_page_if_done_resiliently(ppage, page_size, tpl_sz_d, external_tuple))
		return 0;

	// try simply appending, first
	int res = append_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, external_tuple, abort_error);
	if((*abort_error))
		return 0;
	if(res)
		return 1;

	// else go ahead with compaction, then appending

	// run_page_compaction, i.e. defragment the page
	run_persistent_page_compaction(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, abort_error);
	if((*abort_error))
		return 0;

	// now try to append, this must succeed, unless for an abort_error
	return append_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, external_tuple, abort_error);
}

int insert_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	// if can not insert even resiliently, we fail right here
	if(!can_insert_tuple_on_persistent_page_if_done_resiliently(ppage, page_size, tpl_sz_d, index, external_tuple))
		return 0;

	// try simply inserting, first
	int res = insert_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, external_tuple, abort_error);
	if((*abort_error))
		return 0;
	if(res)
		return 1;

	// else go ahead with compaction, then inserting

	// run_page_compaction, i.e. defragment the page
	run_persistent_page_compaction(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, abort_error);
	if((*abort_error))
		return 0;

	// now try to insert, this must succeed, unless for an abort_error
	return insert_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, external_tuple, abort_error);
}

int update_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	// if can not update even resiliently, we fail right here
	if(!can_update_tuple_on_persistent_page_if_done_resiliently(ppage, page_size, tpl_sz_d, index, external_tuple))
		return 0;

	// try simply updating, first
	int res = update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, external_tuple, abort_error);
	if((*abort_error))
		return 0;
	if(res)
		return 1;

	// else go ahead with compaction without the index-th tuple, then updating

	// place tomb_stone for the old tuple at the index
	// a NULL or a tombstone for a tuple will allow the page to recapture the space taken up by this tuple, can hence give us back more space for the actual update
	update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, NULL, abort_error);
	if((*abort_error))
		return 0;

	// run_page_compaction, i.e. defragment the page
	run_persistent_page_compaction(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, abort_error);
	if((*abort_error))
		return 0;

	// then at the end attempt to update the tuple again
	// this time it must succeed, unless for an abort_error
	return update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, page_size, tpl_sz_d, index, external_tuple, abort_error);
}