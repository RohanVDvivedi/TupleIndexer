#include<linked_page_list_node_util.h>

#include<linked_page_list_page_header.h>

#include<persistent_page_functions.h>

int init_linked_page_list_page(persistent_page* ppage, int is_self_referencing, const linked_page_list_tuple_defs* lpltd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, lpltd_p->pas_p->page_size, sizeof_LINKED_PAGE_LIST_PAGE_HEADER(lpltd_p), &(lpltd_p->record_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage, lpltd_p);
	hdr.parent.type = LINKED_PAGE_LIST_PAGE;
	hdr.next_page_id = (is_self_referencing ? ppage->page_id : lpltd_p->pas_p->NULL_PAGE_ID);
	hdr.prev_page_id = (is_self_referencing ? ppage->page_id : lpltd_p->pas_p->NULL_PAGE_ID);
	set_linked_page_list_page_header(ppage, &hdr, lpltd_p, pmm_p, transaction_id, abort_error);
	if((*abort_error))
		return 0;

	return 1;
}

void print_linked_page_list_page(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p)
{
	print_linked_page_list_page_header(ppage, lpltd_p);
	print_persistent_page(ppage, lpltd_p->pas_p->page_size, lpltd_p->record_def);
}

int is_next_of_in_linked_page_list(const persistent_page* ppage, const persistent_page* ppage_test_next, const linked_page_list_tuple_defs* lpltd_p)
{
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage, lpltd_p);
	return hdr.next_page_id == ppage_test_next->page_id;
}

int is_prev_of_in_linked_page_list(const persistent_page* ppage, const persistent_page* ppage_test_prev, const linked_page_list_tuple_defs* lpltd_p)
{
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage, lpltd_p);
	return hdr.prev_page_id == ppage_test_prev->page_id;
}

int is_head_page_of_linked_page_list(const persistent_page* ppage, uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p)
{
	// check if the page_id of ppage is head_page_id
	return ppage->page_id == head_page_id;
}

int is_tail_page_of_linked_page_list(const persistent_page* ppage, uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p)
{
	// check if the next_page_id of ppage is head_page_id
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage, lpltd_p);
	return hdr.next_page_id == head_page_id;
}

int is_only_head_linked_page_list(const persistent_page* ppage_head, const linked_page_list_tuple_defs* lpltd_p)
{
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage_head, lpltd_p);
	// the page if it is the only node, must point to itself
	return hdr.next_page_id == hdr.prev_page_id && hdr.next_page_id == ppage_head->page_id;
}

int is_dual_node_linked_page_list(const persistent_page* ppage_head, const linked_page_list_tuple_defs* lpltd_p)
{
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage_head, lpltd_p);
	// for a dual node linked_page_list, the next and prev of head (and the other node) both point to some other node (not itself)
	return hdr.next_page_id == hdr.prev_page_id && hdr.next_page_id != ppage_head->page_id;
}

int is_free_linked_page_list_node(const persistent_page* ppage_head, const linked_page_list_tuple_defs* lpltd_p)
{
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage_head, lpltd_p);
	// a node is free if it's next and prev both point to NULL_PAGE_ID
	return hdr.next_page_id == lpltd_p->pas_p->NULL_PAGE_ID && hdr.prev_page_id == lpltd_p->pas_p->NULL_PAGE_ID;
}

persistent_page lock_and_get_next_page_in_linked_page_list(const persistent_page* ppage, int lock_type, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// calling this function is an error, if it is the only node in the linked_page_list
	if(is_only_head_linked_page_list(ppage, lpltd_p))
		return get_NULL_persistent_page(pam_p);

	// get next_page_id and get lock on this next page
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage, lpltd_p);
	return acquire_persistent_page_with_lock(pam_p, transaction_id, hdr.next_page_id, lock_type, abort_error);
}

persistent_page lock_and_get_prev_page_in_linked_page_list(const persistent_page* ppage, int lock_type, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// calling this function is an error, if it is the only node in the linked_page_list
	if(is_only_head_linked_page_list(ppage, lpltd_p))
		return get_NULL_persistent_page(pam_p);

	// get prev_page_id and get lock on this next page
	linked_page_list_page_header hdr = get_linked_page_list_page_header(ppage, lpltd_p);
	return acquire_persistent_page_with_lock(pam_p, transaction_id, hdr.prev_page_id, lock_type, abort_error);
}

int insert_page_in_between_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2,  persistent_page* ppage_to_ins, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the linked_page_list has no nodes existing
	if(ppage_xist1 == NULL && ppage_xist2 == NULL)
	{
		linked_page_list_page_header hdr_to_ins = get_linked_page_list_page_header(ppage_to_ins, lpltd_p);

		// in this case we will just insert the ppage_to_ins as a singular head
		hdr_to_ins.next_page_id = ppage_to_ins->page_id;
		hdr_to_ins.prev_page_id = ppage_to_ins->page_id;

		set_linked_page_list_page_header(ppage_to_ins, &hdr_to_ins, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;

		return 1;
	}
	// this implies that the node page_xist1 must be singular head in the linked_page_list
	else if(ppage_xist1 == ppage_xist2 && is_only_head_linked_page_list(ppage_xist1, lpltd_p))
	{
		linked_page_list_page_header hdr1 = get_linked_page_list_page_header(ppage_xist1, lpltd_p);

		linked_page_list_page_header hdr_to_ins = get_linked_page_list_page_header(ppage_to_ins, lpltd_p);

		// perform pointer manipulations, to make it into a dual-node linked_page_list
		hdr1.next_page_id = ppage_to_ins->page_id;
		hdr1.prev_page_id = ppage_to_ins->page_id;
		hdr_to_ins.next_page_id = ppage_xist1->page_id;
		hdr_to_ins.prev_page_id = ppage_xist1->page_id;

		set_linked_page_list_page_header(ppage_xist1, &hdr1, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;
		set_linked_page_list_page_header(ppage_to_ins, &hdr_to_ins, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;

		return 1;
	}
	// ppage_xist1 and ppage_xist2 are 2 different locked persistent pages
	else if(ppage_xist1 != ppage_xist2)
	{
		linked_page_list_page_header hdr1 = get_linked_page_list_page_header(ppage_xist1, lpltd_p);
		linked_page_list_page_header hdr2 = get_linked_page_list_page_header(ppage_xist2, lpltd_p);

		// ensure valid params
		if(hdr1.next_page_id != ppage_xist2->page_id || hdr2.prev_page_id != ppage_xist1->page_id)
			return 0;

		linked_page_list_page_header hdr_to_ins = get_linked_page_list_page_header(ppage_to_ins, lpltd_p);

		// perform pointer manipulations
		hdr1.next_page_id = ppage_to_ins->page_id;
		hdr2.prev_page_id = ppage_to_ins->page_id;
		hdr_to_ins.next_page_id = ppage_xist2->page_id;
		hdr_to_ins.prev_page_id = ppage_xist1->page_id;

		set_linked_page_list_page_header(ppage_xist1, &hdr1, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;
		set_linked_page_list_page_header(ppage_xist2, &hdr2, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;
		set_linked_page_list_page_header(ppage_to_ins, &hdr_to_ins, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;

		return 1;
	}
	else
		return 0;
}

int remove_page_from_between_linked_page_list(persistent_page* ppage_xist1, persistent_page* ppage_xist2, persistent_page* ppage_xist3, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(ppage_xist1 == ppage_xist2 && ppage_xist2 == ppage_xist3 && is_only_head_linked_page_list(ppage_xist2, lpltd_p)) // removal of the only node of the linked_page_list
	{
		linked_page_list_page_header hdr2 = get_linked_page_list_page_header(ppage_xist2, lpltd_p);

		hdr2.prev_page_id = lpltd_p->pas_p->NULL_PAGE_ID;
		hdr2.next_page_id = lpltd_p->pas_p->NULL_PAGE_ID;

		set_linked_page_list_page_header(ppage_xist2, &hdr2, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;

		return 1;
	}
	else if(ppage_xist1 == ppage_xist3) // removing a node from dual node linked_page_list
	{
		linked_page_list_page_header hdr1 = get_linked_page_list_page_header(ppage_xist1, lpltd_p);
		linked_page_list_page_header hdr2 = get_linked_page_list_page_header(ppage_xist2, lpltd_p);

		// make sure that it is a dual node linked_page_list, consisting of ppage_xist1 and ppage_xist2
		if(!(hdr2.next_page_id == ppage_xist1->page_id && hdr2.prev_page_id == ppage_xist1->page_id
		&& hdr1.next_page_id == ppage_xist2->page_id && hdr1.prev_page_id == ppage_xist2->page_id))
			return 0;

		// perform pointer manipulations, to make ppage_xist1 as a singular head
		hdr1.next_page_id = ppage_xist1->page_id;
		hdr1.prev_page_id = ppage_xist1->page_id;
		hdr2.prev_page_id = lpltd_p->pas_p->NULL_PAGE_ID;
		hdr2.next_page_id = lpltd_p->pas_p->NULL_PAGE_ID;

		set_linked_page_list_page_header(ppage_xist1, &hdr1, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;
		set_linked_page_list_page_header(ppage_xist2, &hdr2, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;

		return 1;
	}
	else if(ppage_xist1 != ppage_xist2 && ppage_xist2 != ppage_xist3 && ppage_xist1 != ppage_xist3) // all 3 are distinct nodes
	{
		linked_page_list_page_header hdr1 = get_linked_page_list_page_header(ppage_xist1, lpltd_p);
		linked_page_list_page_header hdr2 = get_linked_page_list_page_header(ppage_xist2, lpltd_p);
		linked_page_list_page_header hdr3 = get_linked_page_list_page_header(ppage_xist3, lpltd_p);

		// ensure valid params
		if(!(hdr1.next_page_id == ppage_xist2->page_id && hdr2.next_page_id == ppage_xist3->page_id 
		 && hdr3.prev_page_id == ppage_xist2->page_id && hdr2.prev_page_id == ppage_xist1->page_id))
			return 0;

		// perform pointer manipulations
		hdr1.next_page_id = ppage_xist3->page_id;
		hdr3.prev_page_id = ppage_xist1->page_id;
		hdr2.prev_page_id = lpltd_p->pas_p->NULL_PAGE_ID;
		hdr2.next_page_id = lpltd_p->pas_p->NULL_PAGE_ID;

		set_linked_page_list_page_header(ppage_xist1, &hdr1, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;
		set_linked_page_list_page_header(ppage_xist2, &hdr2, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;
		set_linked_page_list_page_header(ppage_xist3, &hdr3, lpltd_p, pmm_p, transaction_id, abort_error);
		if((*abort_error))
			return 0;

		return 1;
	}
	else // 
		return 0;
}
