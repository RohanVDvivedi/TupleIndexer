#include<tupleindexer/worm/worm_page_util.h>

#include<tupleindexer/worm/worm_head_page_header.h>
#include<tupleindexer/worm/worm_any_page_header.h>
#include<tupleindexer/worm/worm_page_header.h>

int init_worm_head_page(persistent_page* ppage, uint32_t reference_counter, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, wtd_p->pas_p->page_size, sizeof_WORM_HEAD_PAGE_HEADER(wtd_p), &(wtd_p->partial_binary_tuple_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	worm_head_page_header hdr = get_worm_head_page_header(ppage, wtd_p);
	hdr.parent.type = WORM_HEAD_PAGE;
	hdr.reference_counter = reference_counter;
	hdr.dependent_root_page_id = dependent_root_page_id;
	hdr.tail_page_id = ppage->page_id;
	hdr.next_page_id = wtd_p->pas_p->NULL_PAGE_ID;
	set_worm_head_page_header(ppage, &hdr, wtd_p, pmm_p, transaction_id, abort_error);
	if((*abort_error))
		return 0;

	return 1;
}

int init_worm_any_page(persistent_page* ppage, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, wtd_p->pas_p->page_size, sizeof_WORM_ANY_PAGE_HEADER(wtd_p), &(wtd_p->partial_binary_tuple_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	worm_any_page_header hdr = get_worm_any_page_header(ppage, wtd_p);
	hdr.parent.type = WORM_ANY_PAGE;
	hdr.next_page_id = wtd_p->pas_p->NULL_PAGE_ID;
	set_worm_any_page_header(ppage, &hdr, wtd_p, pmm_p, transaction_id, abort_error);
	if((*abort_error))
		return 0;

	return 1;
}

void print_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	if(is_worm_head_page(ppage, wtd_p))
		print_worm_head_page_header(ppage, wtd_p);
	else
		print_worm_any_page_header(ppage, wtd_p);
	print_persistent_page(ppage, wtd_p->pas_p->page_size, wtd_p->partial_binary_tuple_def);
}

uint32_t binary_bytes_appendable_on_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	// space_unused = space_allotted - space_occupied
	uint32_t unused_space_on_page = get_space_allotted_to_all_tuples_on_persistent_page(ppage, wtd_p->pas_p->page_size, &(wtd_p->partial_binary_tuple_def->size_def)) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, wtd_p->pas_p->page_size, &(wtd_p->partial_binary_tuple_def->size_def));

	// additional_space_per_tuple_on_page + minimum_size_of_tuple
	uint32_t management_space_to_be_used_for_any_number_of_binary_bytes = get_additional_space_overhead_per_tuple_on_persistent_page(wtd_p->pas_p->page_size, &(wtd_p->partial_binary_tuple_def->size_def)) + get_minimum_tuple_size(wtd_p->partial_binary_tuple_def);

	// to fit a non empty binary,
	// the unused_space_on_page must be greater than required management_space
	if(unused_space_on_page <= management_space_to_be_used_for_any_number_of_binary_bytes)
		return 0;

	// return non zero number of raw binary bytes that can be appended to this page
	return unused_space_on_page - management_space_to_be_used_for_any_number_of_binary_bytes;
}

int update_next_page_id_on_worm_page(persistent_page* ppage, uint64_t next_page_id, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(is_worm_head_page(ppage, wtd_p))
	{
		worm_head_page_header hdr = get_worm_head_page_header(ppage, wtd_p);
		hdr.next_page_id = next_page_id;
		set_worm_head_page_header(ppage, &hdr, wtd_p, pmm_p, transaction_id, abort_error);
	}
	else
	{
		worm_any_page_header hdr = get_worm_any_page_header(ppage, wtd_p);
		hdr.next_page_id = next_page_id;
		set_worm_any_page_header(ppage, &hdr, wtd_p, pmm_p, transaction_id, abort_error);
	}

	if(*abort_error)
		goto ABORT_ERROR;

	return 1;

	ABORT_ERROR:;
	return 0;
}