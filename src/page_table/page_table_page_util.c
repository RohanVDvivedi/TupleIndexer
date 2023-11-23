#include<page_table_page_util.h>

#include<page_table_page_header.h>

int init_page_table_page(persistent_page* ppage, uint32_t level, uint64_t first_bucket_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, pttd_p->pas_p->page_size, sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), &(pttd_p->entry_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	page_table_page_header hdr = get_page_table_page_header(ppage, pttd_p);
	hdr.parent.type = PAGE_TABLE_PAGE;
	hdr.level = level;
	hdr.first_bucket_id = first_bucket_id;
	set_page_table_page_header(ppage, &hdr, pttd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}

uint64_t get_child_page_id_at_child_index_in_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

int set_child_page_id_at_child_index_in_page_table_page(const persistent_page* ppage, uint64_t child_page_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int has_all_NULL_PAGE_ID_in_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);
