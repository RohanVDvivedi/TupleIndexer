#include<linked_page_list.h>

#include<linked_page_list_page_header.h>
#include<linked_page_list_node_util.h>

#include<persistent_page_functions.h>

uint64_t get_new_linked_page_list(const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page head_page = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);

	// failure to acquire a new page
	if(*abort_error)
		return lpltd_p->pas_p->NULL_PAGE_ID;

	// init_page as a self referencing linked_page_list page
	// if init_page fails
	init_linked_page_list_page(&head_page, 1, lpltd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
		return lpltd_p->pas_p->NULL_PAGE_ID;
	}

	uint64_t res = head_page.page_id;
	release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
	if(*abort_error)
		return lpltd_p->pas_p->NULL_PAGE_ID;

	return res;
}

int destroy_linked_page_list(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	uint64_t page_id = head_page_id;

	do
	{
		// get lock on the page_id
		persistent_page ppage = acquire_persistent_page_with_lock(pam_p, transaction_id, page_id, READ_LOCK, abort_error);
		if(*abort_error)
			return 0;

		// incrementing iterator to free next page in next iteration
		page_id = get_next_page_id_of_linked_page_list_page(&ppage, lpltd_p);

		// attempt to free the page, if it fails, just release lock and exit with failure
		release_lock_on_persistent_page(pam_p, transaction_id, &ppage, FREE_PAGE, abort_error);
		if(*abort_error)
			release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
	}
	while(page_id != head_page_id); // continue until you reach head_page again

	return 1;
}