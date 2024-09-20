#include<bplus_tree.h>

#include<bplus_tree_walk_down.h>
#include<bplus_tree_split_insert_util.h>
#include<persistent_page_functions.h>

int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(!check_if_record_can_be_inserted_for_bplus_tree_tuple_definitions(bpttd_p, record))
		return 0;

	// create a locked_pages_stack
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	(*locked_pages_stack_p) = initialize_locked_pages_stack_for_walk_down(root_page_id, WRITE_LOCK, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error) // on abort no pages were kept locked
		return 0;

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