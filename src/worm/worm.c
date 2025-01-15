#include<worm.h>

#include<persistent_page_functions.h>

#include<worm_page_util.h>
#include<worm_head_page_header.h>
#include<worm_any_page_header.h>
#include<worm_page_header.h>

uint64_t get_new_worm(uint64_t reference_counter, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(reference_counter == 0)
		reference_counter = 1;

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
	persistent_page head_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		return 0;

	worm_head_page_header hdr = get_worm_head_page_header(&head_page, wtd_p);
	hdr.reference_counter--;
	set_worm_head_page_header(&head_page, &hdr, wtd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
		return 0;
	}

	// if it is still referenced, nothing needs to be done, except releasing the lock on the head_page
	if(hdr.reference_counter > 0)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &head_page, NONE_OPTION, abort_error);
		if(*abort_error)
			return 0;

		return 1;
	}

	// destroy the data_structue at dependent_root_page_id here
	// TODO 

	// giving a new name to the local variable for iteration
	persistent_page* curr_page = &head_page;

	while(1)
	{
		uint64_t next_page_id = get_next_page_id_of_worm_page(curr_page, wtd_p);

		// free curr_page
		release_lock_on_persistent_page(pam_p, transaction_id, curr_page, FREE_PAGE, abort_error);
		if(*abort_error)
		{
			// if the free fails we still need to release lock on it
			release_lock_on_persistent_page(pam_p, transaction_id, curr_page, NONE_OPTION, abort_error);
			return 0;
		}

		// if there is no next page, break out of the loop
		if(next_page_id == wtd_p->pas_p->NULL_PAGE_ID)
			break;

		// acquire lock on the next_page
		(*curr_page) = acquire_persistent_page_with_lock(pam_p, transaction_id, next_page_id, READ_LOCK, abort_error);
		if(*abort_error)
			return 0;
	}

	return 1;
}

void print_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// print the head page id of the worm
	printf("\n\nWorm @ head_page_id = %"PRIu64"\n\n", head_page_id);

	// start with acquiring lock on the head page
	persistent_page curr_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, READ_LOCK, abort_error);
	if(*abort_error)
		return;

	// loop until the curr_page is not NULL
	while(!is_persistent_page_NULL(&curr_page, pam_p))
	{
		// print the curr_page
		printf("page_id : %"PRIu64"\n\n", curr_page.page_id);
		print_worm_page(&curr_page, wtd_p);
		printf("xxxxxxxxxxxxx\n");

		// get the next page_id
		uint64_t next_page_id = get_next_page_id_of_worm_page(&curr_page, wtd_p);

		// if there is next page, acquire lock on it
		persistent_page next_page = get_NULL_persistent_page(pam_p);
		if(next_page_id != wtd_p->pas_p->NULL_PAGE_ID)
		{
			next_page = acquire_persistent_page_with_lock(pam_p, transaction_id, next_page_id, READ_LOCK, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
				return;
			}
		}

		// release lock on the curr_page
		release_lock_on_persistent_page(pam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
		if(*abort_error)
		{
			if(!is_persistent_page_NULL(&next_page, pam_p))
				release_lock_on_persistent_page(pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
			return;
		}

		// next_page becomes the curr_page
		curr_page = next_page;
	}
}