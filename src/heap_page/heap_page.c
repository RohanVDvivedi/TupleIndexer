#include<tupleindexer/heap_page/heap_page.h>

#include<tupleindexer/heap_page/heap_page_header.h>

persistent_page get_new_heap_page_with_write_lock(const page_access_specs* pas_p, const tuple_def* tpl_d, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page heap_page = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);
	if(*abort_error)
		return heap_page;

	int inited = init_persistent_page(pmm_p, transaction_id, &heap_page, pas_p->page_size, sizeof_HEAP_PAGE_HEADER(pas_p), &(tpl_d->size_def), abort_error);
	if((*abort_error) || !inited)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, NONE_OPTION, abort_error);
		return heap_page;
	}

	// get the header, initialize it and set it back on to the page
	heap_page_header hdr = get_heap_page_header(&heap_page, pas_p);
	hdr.parent.type = HEAP_PAGE; // always a heap page
	set_heap_page_header(&heap_page, &hdr, pas_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, NONE_OPTION, abort_error);
		return heap_page;
	}

	return heap_page;
}

uint32_t get_unused_space_on_heap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d)
{
	return get_space_allotted_to_all_tuples_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def)) - get_space_occupied_by_all_tuples_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def));
}

int is_heap_page_empty(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d)
{
	return get_tomb_stone_count_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def)) == 
			get_tuple_count_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def));
}

void print_heap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d)
{
	print_heap_page_header(ppage, pas_p);
	print_persistent_page(ppage, pas_p->page_size, tpl_d);
}