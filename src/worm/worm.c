#include<worm.h>

#include<persistent_page_functions.h>

#include<worm_page_util.h>

uint64_t get_new_worm(uint64_t reference_counter, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page head_page = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);

	// failure to acquire a new page
	if(*abort_error)
		return wtd_p->pas_p->NULL_PAGE_ID;

	// init head page
	init_worm_head_page(&head_page, reference_counter, dependent_root_page_id, wtd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
		return wtd_p->pas_p->NULL_PAGE_ID;
	}

	uint64_t res = head_page.page_id;
	release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
	if(*abort_error)
		return wtd_p->pas_p->NULL_PAGE_ID;

	return res;
}

int increment_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page head_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		return 0;

	worm_head_page_header hdr = get_worm_head_page_header(&head_page, wtd_p);
	hdr.reference_counter++;
	set_worm_head_page_header(&head_page, &hdr, wtd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
		return 0;
	}

	release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}

int decrement_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

void print_worm(uint64_t root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// TODO
}