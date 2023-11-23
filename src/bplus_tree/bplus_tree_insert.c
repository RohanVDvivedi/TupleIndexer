#include<bplus_tree.h>

#include<locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_page_header.h>
#include<persistent_page_functions.h>
#include<bplus_tree_split_insert_util.h>

#include<stdlib.h>

int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(!check_if_record_can_be_inserted_into_bplus_tree(bpttd_p, record))
		return 0;

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	{
		// get lock on the root page of the bplus_tree
		persistent_page root_page = acquire_persistent_page_with_lock(pam_p, transaction_id, root_page_id, WRITE_LOCK, abort_error);
		if(*abort_error)
			return 0;

		// pre cache level of the root_page
		uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// create a stack of capacity = levels
		if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
			exit(-1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, INVALID_TUPLE_INDEX));
	}

	// walk down taking locks until you reach leaf page level
	walk_down_locking_parent_pages_for_split_insert_using_record(root_page_id, locked_pages_stack_p, record, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
	{
		deinitialize_locked_pages_stack(locked_pages_stack_p);
		return 0;
	}

	int split_inserted = split_insert_and_unlock_pages_up(root_page_id, locked_pages_stack_p, record, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);

	// on a abort_error here, the corresponding locks to the pages in the locked_pages_stack would have already been released by split_insert_and_unlock_pages_up
	deinitialize_locked_pages_stack(locked_pages_stack_p);

	return split_inserted;
}