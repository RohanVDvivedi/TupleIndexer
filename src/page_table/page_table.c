#include<page_table.h>

#include<page_table_page_util.h>
#include<persistent_page_functions.h>

uint64_t get_new_page_table(const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page root_page = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);

	// failure to acquire a new page
	if(*abort_error)
		return pttd_p->pas_p->NULL_PAGE_ID;

	// init root page as if it was the first leaf page
	init_page_table_page(&root_page, 0, 0, pttd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
		return pttd_p->pas_p->NULL_PAGE_ID;
	}

	uint64_t res = root_page.page_id;
	release_lock_on_persistent_page(pam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
	if(*abort_error)
		return pttd_p->pas_p->NULL_PAGE_ID;

	return res;
}

int destroy_page_table(uint64_t root_page_id, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

void print_page_table(uint64_t root_page_id, int only_leaf_pages, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// TODO
}