#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<tupleindexer/bplus_tree/bplus_tree_walk_down.h>
#include<tupleindexer/bplus_tree/bplus_tree_split_insert_util.h>
#include<tupleindexer/utils/persistent_page_functions.h>
#include<tupleindexer/utils/sorted_packed_page_util.h>

int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inserted = 0;

	if(!check_if_record_can_be_inserted_for_bplus_tree_tuple_definitions(bpttd_p, record))
		return 0;

	// create a locked_pages_stack
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	(*locked_pages_stack_p) = initialize_locked_pages_stack_for_walk_down(root_page_id, WRITE_LOCK, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error) // on abort no pages were kept locked
		return 0;

	// walk down taking locks until you reach leaf page level
	walk_down_locking_parent_pages_for_split_insert_using_record(locked_pages_stack_p, record, bpttd_p, pam_p, transaction_id, abort_error);
	if(*abort_error)
		goto EXIT;

	// this has to be a leaf page
	locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

	uint32_t insertion_index = find_insertion_point_in_sorted_packed_page(
										&(curr_locked_page->ppage), bpttd_p->pas_p->page_size,
										bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
										record
									);

	// insertion_index is always the index right after all tuples lesser than equal to the record

	if(insertion_index > 0)
	{
		// find index of last record that has the matching key on the page
		// the greatest record lesser than or equal to this key on this page must not have the same key
		uint32_t found_index = insertion_index - 1;

		// make sure tuple is not having a duplicate key
		if(0 == compare_tuples(record, bpttd_p->record_def, bpttd_p->key_element_ids,
			get_nth_tuple_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def), found_index), bpttd_p->record_def, bpttd_p->key_element_ids,
			bpttd_p->key_compare_direction, bpttd_p->key_element_count))
			goto EXIT;
	}

	inserted = split_insert_and_unlock_pages_up(root_page_id, locked_pages_stack_p, record, insertion_index, bpttd_p, pam_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto EXIT;

	EXIT:;
	release_all_locks_and_deinitialize_stack_reenterable(locked_pages_stack_p, pam_p, transaction_id, abort_error);

	if(*abort_error)
		return 0;

	return inserted;
}