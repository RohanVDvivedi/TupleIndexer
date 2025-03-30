#include<tupleindexer/linked_page_list/linked_page_list.h>

#include<tupleindexer/linked_page_list/linked_page_list_page_header.h>
#include<tupleindexer/linked_page_list/linked_page_list_node_util.h>

#include<tupleindexer/utils/persistent_page_functions.h>

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
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
			return 0;
		}
	}
	while(page_id != head_page_id); // continue until you reach head_page again

	return 1;
}

void print_linked_page_list(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// print the head page id of the linked page list
	printf("\n\nLinked_page_list @ head_page_id = %"PRIu64"\n\n", head_page_id);

	// lock the head page, it always exists
	persistent_page ppage = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, READ_LOCK, abort_error);
	if(*abort_error)
		return;

	while(1)
	{
		// print this page and its page_id
		printf("page_id : %"PRIu64"\n\n", ppage.page_id);
		print_linked_page_list_page(&ppage, lpltd_p);
		printf("xxxxxxxxxxxxx\n\n");

		// get it's next_page_id
		uint64_t next_page_id = get_next_page_id_of_linked_page_list_page(&ppage, lpltd_p);

		if(next_page_id == head_page_id) // i.e. this ppage is the tail page, so this was the last page to be processed
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
			if(*abort_error)
				return;
			break;
		}
		else
		{
			persistent_page next_ppage = acquire_persistent_page_with_lock(pam_p, transaction_id, next_page_id, READ_LOCK, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
				return;
			}

			release_lock_on_persistent_page(pam_p, transaction_id, &ppage, NONE_OPTION, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &next_ppage, NONE_OPTION, abort_error);
				return;
			}

			ppage = next_ppage;
		}
	}
}

int merge_linked_page_lists(uint64_t lpl1_head_page_id, uint64_t lpl2_head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// preinitialize all variables
	persistent_page lpl1_head = get_NULL_persistent_page(pam_p);
	persistent_page lpl1_tail = get_NULL_persistent_page(pam_p);
	persistent_page* lpl1_tail_p = NULL;

	persistent_page lpl2_head = get_NULL_persistent_page(pam_p);
	persistent_page lpl2_tail = get_NULL_persistent_page(pam_p);
	persistent_page* lpl2_tail_p = NULL;

	int result = 0;

	// get necessary locks
	lpl1_head = acquire_persistent_page_with_lock(pam_p, transaction_id, lpl1_head_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	lpl2_head = acquire_persistent_page_with_lock(pam_p, transaction_id, lpl2_head_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// if lpl2 is empty, discard lpl2 completely
	if(is_only_head_linked_page_list(&lpl2_head, lpltd_p) && 0 == get_tuple_count_on_persistent_page(&lpl2_head, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def)))
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head, FREE_PAGE, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		result = 1;
		goto EXIT;
	}

	// if lpl1 is empty - very complex case
	if(is_only_head_linked_page_list(&lpl1_head, lpltd_p) && 0 == get_tuple_count_on_persistent_page(&lpl1_head, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def)))
	{
		// copy all lpl2_head tuples to lpl1_head
		for(uint32_t i = 0; i < get_tuple_count_on_persistent_page(&lpl2_head, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def)); i++)
		{
			const void* ith_tuple = get_nth_tuple_on_persistent_page(&lpl2_head, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), i);
			append_tuple_on_persistent_page_resiliently(pmm_p, transaction_id, &lpl1_head, lpltd_p->pas_p->page_size, &(lpltd_p->record_def->size_def), ith_tuple, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		if(is_dual_node_linked_page_list(&lpl2_head, lpltd_p))
		{
			// grab lock on the next of lpl2
			persistent_page lpl2_head_next = lock_and_get_next_page_in_linked_page_list(&lpl2_head, WRITE_LOCK, lpltd_p, pam_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// remove lpl2 from between of next and next
			remove_page_from_between_linked_page_list(&lpl2_head_next, &lpl2_head, &lpl2_head_next, lpltd_p, pam_p, pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_next, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// insert lpl1 in between next and next
			insert_page_in_between_linked_page_list(&lpl2_head_next, &lpl2_head_next, &lpl1_head, lpltd_p, pam_p, pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_next, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// release lock on next
			release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_next, NONE_OPTION, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
		else if(!is_only_head_linked_page_list(&lpl2_head, lpltd_p)) // coming here means it is surely not dual node, if it is also not only_head then it must be many node linked_page_list
		{
			// grab lock on the next and prev of lpl2
			persistent_page lpl2_head_next = lock_and_get_next_page_in_linked_page_list(&lpl2_head, WRITE_LOCK, lpltd_p, pam_p, transaction_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			persistent_page lpl2_head_prev = lock_and_get_prev_page_in_linked_page_list(&lpl2_head, WRITE_LOCK, lpltd_p, pam_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_next, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// remove lpl2 from between of next and prev
			remove_page_from_between_linked_page_list(&lpl2_head_prev, &lpl2_head, &lpl2_head_next, lpltd_p, pam_p, pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_next, NONE_OPTION, abort_error);
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_prev, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// insert lpl1 in between next and prev
			insert_page_in_between_linked_page_list(&lpl2_head_prev, &lpl2_head_next, &lpl1_head, lpltd_p, pam_p, pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_next, NONE_OPTION, abort_error);
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_prev, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// release lock on next and prev
			release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_next, NONE_OPTION, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_prev, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head_prev, NONE_OPTION, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// free lpl2_head
		release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head, FREE_PAGE, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		result = 1;
		goto EXIT;
	}

	if(is_only_head_linked_page_list(&lpl1_head, lpltd_p))
		lpl1_tail_p = &lpl1_head;
	else
	{
		lpl1_tail = lock_and_get_prev_page_in_linked_page_list(&lpl1_head, WRITE_LOCK, lpltd_p, pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		lpl1_tail_p = &lpl1_tail;
	}

	if(is_only_head_linked_page_list(&lpl2_head, lpltd_p))
		lpl2_tail_p = &lpl2_head;
	else
	{
		lpl2_tail = lock_and_get_prev_page_in_linked_page_list(&lpl2_head, WRITE_LOCK, lpltd_p, pam_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		lpl2_tail_p = &lpl2_tail;
	}

	// case when both of them are not empty
	linked_page_list_page_header lpl1_head_hdr = get_linked_page_list_page_header(&lpl1_head, lpltd_p);
	lpl1_head_hdr.prev_page_id = lpl2_tail_p->page_id;
	set_linked_page_list_page_header(&lpl1_head, &lpl1_head_hdr, lpltd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	linked_page_list_page_header lpl1_tail_hdr = get_linked_page_list_page_header(lpl1_tail_p, lpltd_p);
	lpl1_tail_hdr.next_page_id = lpl2_head.page_id;
	set_linked_page_list_page_header(lpl1_tail_p, &lpl1_tail_hdr, lpltd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	linked_page_list_page_header lpl2_head_hdr = get_linked_page_list_page_header(&lpl2_head, lpltd_p);
	lpl2_head_hdr.prev_page_id = lpl1_tail_p->page_id;
	set_linked_page_list_page_header(&lpl2_head, &lpl2_head_hdr, lpltd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	linked_page_list_page_header lpl2_tail_hdr = get_linked_page_list_page_header(lpl2_tail_p, lpltd_p);
	lpl2_tail_hdr.next_page_id = lpl1_head.page_id;
	set_linked_page_list_page_header(lpl2_tail_p, &lpl2_tail_hdr, lpltd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	result = 1;
	goto EXIT;

	// clean up
	ABORT_ERROR:;
	EXIT:;
	if(!is_persistent_page_NULL(&lpl1_head, pam_p))
		release_lock_on_persistent_page(pam_p, transaction_id, &lpl1_head, NONE_OPTION, abort_error);
	if(!is_persistent_page_NULL(&lpl1_tail, pam_p))
		release_lock_on_persistent_page(pam_p, transaction_id, &lpl1_tail, NONE_OPTION, abort_error);
	if(!is_persistent_page_NULL(&lpl2_head, pam_p))
		release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_head, NONE_OPTION, abort_error);
	if(!is_persistent_page_NULL(&lpl2_tail, pam_p))
		release_lock_on_persistent_page(pam_p, transaction_id, &lpl2_tail, NONE_OPTION, abort_error);
	if(*abort_error)
		return 0;
	return result;
}