#include<bplus_tree_walk_down.h>

#include<page_access_methods.h>
#include<bplus_tree_page_header.h>

#include<invalid_tuple_indices.h>

#include<stdlib.h>

locked_pages_stack initialize_locked_pages_stack_for_walk_down(uint64_t root_page_id, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	// get lock on the root page of the bplus_tree
	persistent_page root_page = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, lock_type, abort_error);
	if(*abort_error)
		return ((locked_pages_stack){});

	// get level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

	// create a stack of capacity = levels
	if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
		exit(-1);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, INVALID_TUPLE_INDEX));

	return *locked_pages_stack_p;
}