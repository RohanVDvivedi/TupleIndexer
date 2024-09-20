#include<bplus_tree.h>

#include<bplus_tree_walk_down.h>
#include<sorted_packed_page_util.h>
#include<bplus_tree_merge_util.h>
#include<persistent_page_functions.h>

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int deleted = 0;

	// create a locked_pages_stack
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	(*locked_pages_stack_p) = initialize_locked_pages_stack_for_walk_down(root_page_id, WRITE_LOCK, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error) // on abort no pages were kept locked
		return 0;

	// walk down taking locks until you reach leaf page level
	walk_down_locking_parent_pages_for_merge_using_key(locked_pages_stack_p, key, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
		goto EXIT;

	// this has to be a leaf page
	// we access it using pointer, so that upon deleting, its was modified bit get's set
	locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

	// find index of last record that has the given key on the page
	uint32_t found_index = find_last_in_sorted_packed_page(
										&(curr_locked_page->ppage), bpttd_p->pas_p->page_size,
										bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
										key, bpttd_p->key_def, NULL
									);

	// if no such record can be found, we break and exit
	if(NO_TUPLE_FOUND == found_index)
		goto EXIT;

	// perform a delete operation on the found index in this page, this has to succeed for a valid index
	deleted = delete_in_sorted_packed_page(
						&(curr_locked_page->ppage), bpttd_p->pas_p->page_size,
						bpttd_p->record_def,
						found_index,
						pmm_p,
						transaction_id,
						abort_error
					);
	if(*abort_error)
		goto EXIT;

	merge_and_unlock_pages_up(root_page_id, locked_pages_stack_p, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto EXIT;

	EXIT:;
	release_all_locks_and_deinitialize_stack_reenterable(locked_pages_stack_p, pam_p, transaction_id, abort_error);

	if(*abort_error)
		return 0;

	return deleted;
}