#include<bplus_tree_find_util.h>

#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_page_header.h>
#include<persistent_page_functions.h>

int read_couple_locks_until_leaf_using_key(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, int lock_type, persistent_page* locked_leaf, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
	// get lock on the root page of the bplus_tree
	// root_page may be a leaf page (we can not know its level without locking the page first), so we always lock it with lock_type
	// this will frustratingly kill the performance but, I have no idea how to do better
	persistent_page curr_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, lock_type, abort_error);
	if(*abort_error)
		return 0;

	while(!is_bplus_tree_leaf_page(&curr_page, bpttd_p))
	{
		// get level of the curr_page
		uint32_t curr_page_level = get_level_of_bplus_tree_page(&curr_page, bpttd_p);

		uint32_t child_index = find_child_index_for_key(&curr_page, key, key_element_count_concerned, bpttd_p);

		// get the next page id down the line
		uint64_t next_page_id = get_child_page_id_by_child_index(&curr_page, child_index, bpttd_p);

		// the next_page would be a leaf page, if the curr_page_level == 1
		// if so, lock the next_page with the lock_type instead of READ_LOCK
		persistent_page next_page = acquire_persistent_page_with_lock(dam_p, transaction_id, next_page_id, (curr_page_level == 1) ? lock_type : READ_LOCK, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
			return 0;
		}

		// release lock on the curr_page and 
		// make the next_page as the curr_page
		release_lock_on_persistent_page(dam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
		if(*abort_error) // on an abort error, release lock on the next_page and exit
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
			return 0;
		}
		curr_page = next_page;
	}

	// curr_page is the leaf page at the end of this loop
	*locked_leaf = curr_page;

	return 1;
}

int read_couple_locks_until_leaf_using_record(uint64_t root_page_id, const void* record, uint32_t key_element_count_concerned, int lock_type, persistent_page* locked_leaf, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
	// get lock on the root page of the bplus_tree
	// root_page may be a leaf page (we can not know its level without locking the page first), so we always lock it with lock_type
	// this will frustratingly kill the performance but, I have no idea how to do better
	persistent_page curr_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, lock_type, abort_error);
	if(*abort_error)
		return 0;

	while(!is_bplus_tree_leaf_page(&curr_page, bpttd_p))
	{
		// get level of the curr_page
		uint32_t curr_page_level = get_level_of_bplus_tree_page(&curr_page, bpttd_p);

		uint32_t child_index = find_child_index_for_record(&curr_page, record, key_element_count_concerned, bpttd_p);

		// get the next page id down the line
		uint64_t next_page_id = get_child_page_id_by_child_index(&curr_page, child_index, bpttd_p);

		// the next_page would be a leaf page, if the curr_page_level == 1
		// if so, lock the next_page with the lock_type instead of READ_LOCK
		persistent_page next_page = acquire_persistent_page_with_lock(dam_p, transaction_id, next_page_id, (curr_page_level == 1) ? lock_type : READ_LOCK, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
			return 0;
		}

		// release lock on the curr_page and 
		// make the next_page as the curr_page
		release_lock_on_persistent_page(dam_p, transaction_id, &curr_page, NONE_OPTION, abort_error);
		if(*abort_error) // on an abort error, release lock on the next_page and exit
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
			return 0;
		}
		curr_page = next_page;
	}

	// curr_page is the leaf page at the end of this loop
	*locked_leaf = curr_page;

	return 1;
}