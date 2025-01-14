#include<worm_page_util.h>

#include<worm_head_page_header.h>
#include<worm_any_page_header.h>

int init_worm_head_page(persistent_page* ppage, uint32_t reference_counter, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, wtd_p->pas_p->page_size, sizeof_WORM_HEAD_PAGE_HEADER(wtd_p), &(wtd_p->partial_blob_tuple_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	worm_head_page_header hdr = get_worm_head_page_header(ppage, wtd_p);
	hdr.parent.type = WORM_HEAD_PAGE;
	hdr.reference_counter = reference_counter;
	hdr.dependent_root_page_id = dependent_root_page_id;
	hdr.tail_page_id = wtd_p->pas_p->NULL_PAGE_ID;
	hdr.next_page_id = wtd_p->pas_p->NULL_PAGE_ID;
	set_worm_head_page_header(ppage, &hdr, wtd_p, pmm_p, transaction_id, abort_error);
	if((*abort_error))
		return 0;

	return 1;
}

int init_worm_any_page(persistent_page* ppage, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, wtd_p->pas_p->page_size, sizeof_WORM_ANY_PAGE_HEADER(wtd_p), &(wtd_p->partial_blob_tuple_def->size_def), abort_error);
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

uint32_t blob_bytes_insertable_for_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	// TODO
}