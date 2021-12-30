#include<bplus_tree.h>

#include<sorted_packed_page_util.h>

#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>

#include<storage_capacity_page_util.h>

#include<arraylist.h>

#include<stdlib.h>
#include<string.h>

int init_bplus_tree(bplus_tree_handle* bpth, const tuple_def* record_def, uint16_t key_element_count, uint32_t root_id)
{
	initialize_rwlock(&(bpth->handle_lock));
	bpth->root_id = root_id;

	// initialize all the tuple definitions that will be used in the bplus tree

	// tuple definition initialization fails if
	if(key_element_count == 0 || is_empty_tuple_def(record_def) || key_element_count > record_def->element_count)
		return 0;

	bpth->tuple_definitions.key_def = malloc(size_of_tuple_def(key_element_count));
	memmove(bpth->tuple_definitions.key_def, record_def, size_of_tuple_def(key_element_count));
	bpth->tuple_definitions.key_def->element_count = key_element_count;
	finalize_tuple_def(bpth->tuple_definitions.key_def);

	bpth->tuple_definitions.index_def = malloc(size_of_tuple_def(key_element_count + 1));
	memmove(bpth->tuple_definitions.index_def, record_def, size_of_tuple_def(key_element_count));
	bpth->tuple_definitions.index_def->element_count = key_element_count;
	insert_element_def(bpth->tuple_definitions.index_def, UINT, 4);
	finalize_tuple_def(bpth->tuple_definitions.index_def);

	bpth->tuple_definitions.record_def = malloc(size_of_tuple_def(record_def->element_count));
	memmove(bpth->tuple_definitions.record_def, record_def, size_of_tuple_def(record_def->element_count));
	finalize_tuple_def(bpth->tuple_definitions.record_def);

	return 1;
}

int deinit_bplus_tree(bplus_tree_handle* bpth)
{
	deinitialize_rwlock(&(bpth->handle_lock));
	bpth->root_id = NULL_PAGE_REF;
	free(bpth->tuple_definitions.key_def);
	free(bpth->tuple_definitions.index_def);
	free(bpth->tuple_definitions.record_def);
	return 1;
}

int find_in_bplus_tree(bplus_tree_handle* bpth, const void* key, read_cursor* rc, const data_access_methods* dam_p)
{
	int found = 0;

	read_lock(&(bpth->handle_lock));

	void* curr_page = NULL;
	if(bpth->root_id != NULL_PAGE_REF)
		curr_page = dam_p->acquire_page_with_reader_lock(dam_p->context, bpth->root_id);

	read_unlock(&(bpth->handle_lock));

	while(curr_page != NULL)
	{
		switch(get_page_type(curr_page))
		{
			case LEAF_PAGE_TYPE :
			{
				// find record for the given key in b+tree
				uint16_t found_index;
				found = search_in_sorted_packed_page(curr_page, dam_p->page_size, bpth->tuple_definitions.key_def, bpth->tuple_definitions.record_def, key, &found_index);

				if(!found)
				{
					uint16_t test_index = found_index;
					const void* test_record = get_nth_tuple(curr_page, dam_p->page_size, bpth->tuple_definitions.record_def, test_index);
					int compare = compare_tuples(test_record, key, bpth->tuple_definitions.key_def);

					if(compare > 0)
					{
						while(1)
						{
							test_record = get_nth_tuple(curr_page, dam_p->page_size, bpth->tuple_definitions.record_def, test_index);
							compare = compare_tuples(test_record, key, bpth->tuple_definitions.key_def);
							if(compare > 0)
							{
								found_index = test_index;
								if(test_index > 0)
									test_index--;
								else
									break;
							}
							else
								break;
						}
					}
					else if(compare < 0)
					{
						while(1)
						{
							test_record = get_nth_tuple(curr_page, dam_p->page_size, bpth->tuple_definitions.record_def, test_index);
							compare = compare_tuples(test_record, key, bpth->tuple_definitions.key_def);
							if(compare < 0)
							{
								test_index++;
								if(test_index == get_tuple_count(curr_page))
								{
									found_index = test_index;
									break;
								}
							}
							else
							{
								found_index = test_index;
								break;
							}
						}
					}

				}

				if(rc == NULL)	// release lock and cleanup
					dam_p->release_reader_lock_on_page(dam_p->context, curr_page);
				else
				{
					open_read_cursor(rc, curr_page, found_index, bpth->tuple_definitions.record_def);
					if(found_index == get_tuple_count(curr_page))
						seek_next_read_cursor(rc, dam_p);
				}
				curr_page = NULL;
				break;
			}
			case INTERIOR_PAGE_TYPE :
			{
				// find next page_id to jump to the next level in the b+tree
				int32_t next_indirection_index = find_in_interior_page(curr_page, dam_p->page_size, key, &(bpth->tuple_definitions));
				uint32_t next_page_id = get_index_page_id_from_interior_page(curr_page, dam_p->page_size, next_indirection_index, &(bpth->tuple_definitions));

				// lock next page, prior to releasing the lock on the current page
				void* next_page = dam_p->acquire_page_with_reader_lock(dam_p->context, next_page_id);
				dam_p->release_reader_lock_on_page(dam_p->context, curr_page);

				// reiterate until you reach the leaf page
				curr_page = next_page;
				break;
			}
		}
	}

	return found;
}

static void insert_new_root_node_HANDLE_UNSAFE(bplus_tree_handle* bpth, const void* parent_index_insert, const data_access_methods* dam_p)
{
	uint32_t old_root_id = bpth->root_id;
	
	uint32_t new_root_page_id;
	void* new_root_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_root_page_id);
	init_interior_page(new_root_page, dam_p->page_size, &(bpth->tuple_definitions));

	// insert the only entry for a new root level
	insert_to_sorted_packed_page(new_root_page, dam_p->page_size, bpth->tuple_definitions.key_def, bpth->tuple_definitions.index_def, parent_index_insert, NULL);

	// update all least referenc of this is page
	set_index_page_id_in_interior_page(new_root_page, dam_p->page_size, -1, &(bpth->tuple_definitions), old_root_id);

	dam_p->release_writer_lock_on_page(dam_p->context, new_root_page);

	bpth->root_id = new_root_page_id;
}

int insert_in_bplus_tree(bplus_tree_handle* bpth, const void* record, const data_access_methods* dam_p)
{
	// record size must be lesser than or equal to half the allotted page size
	uint32_t record_size_on_page = get_tuple_size(bpth->tuple_definitions.record_def, record) + get_additional_space_occupied_per_tuple(dam_p->page_size, bpth->tuple_definitions.record_def);
	uint32_t max_record_size_on_page = get_space_to_be_allotted_for_all_tuples(1, dam_p->page_size, bpth->tuple_definitions.record_def) / 2;
	if(record_size_on_page > max_record_size_on_page)
		return 0;

	// or even generated index entry size must be lesser than or equal to half the allotted page size
	uint32_t index_entry_size_on_page = (get_tuple_size(bpth->tuple_definitions.key_def, record) + 4) + get_additional_space_occupied_per_tuple(dam_p->page_size, bpth->tuple_definitions.index_def);
	uint32_t max_index_entry_size_on_page = get_space_to_be_allotted_for_all_tuples(1, dam_p->page_size, bpth->tuple_definitions.index_def) / 2;
	if(index_entry_size_on_page > max_index_entry_size_on_page)
		return 0;

	arraylist locked_parents;
	initialize_arraylist(&locked_parents, 64);

	write_lock(&(bpth->handle_lock));
	int is_handle_locked = 1;

	void* curr_page = NULL;
	if(bpth->root_id != NULL_PAGE_REF)
		curr_page = dam_p->acquire_page_with_writer_lock(dam_p->context, bpth->root_id);
	else // create a new leaf and add it as the root page
	{
		curr_page = dam_p->get_new_page_with_write_lock(dam_p->context, &(bpth->root_id));
		init_leaf_page(curr_page, dam_p->page_size, &(bpth->tuple_definitions));
	}

	void* parent_index_insert = NULL;

	int inserted = 0;

	// quit when no locks are held
	while( ! (is_empty_arraylist(&locked_parents) && curr_page == NULL) )
	{
		switch(get_page_type(curr_page))
		{
			case LEAF_PAGE_TYPE :
			{// no duplicates allowed
				// the key should not be present
				int found = search_in_sorted_packed_page(curr_page, dam_p->page_size, bpth->tuple_definitions.key_def, bpth->tuple_definitions.record_def, record, NULL);

				// try to insert record to the curr_page, only if the key is not found on the page
				if(!found)
					inserted = insert_to_sorted_packed_page(curr_page, dam_p->page_size, bpth->tuple_definitions.key_def, bpth->tuple_definitions.record_def, record, NULL);
				
				// condition to quit the loop => inserted (success) || found (failure)
				if(inserted || found)
				{
					while(!is_empty_arraylist(&locked_parents))
					{
						void* some_parent = (void*) get_front_of_arraylist(&locked_parents);
						dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
						pop_front_from_arraylist(&locked_parents);
					}

					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);
					curr_page = NULL;
				}
				// insert forcefully with a split
				else
				{
					// get new blank page, to split this page into
					uint32_t new_page_id;
					void* new_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_page_id);
					
					// split and insert to this leaf node and propogate the parent_index_insert, don not forget to set the inserted flag
					parent_index_insert = split_insert_leaf_page(curr_page, record, new_page, new_page_id, dam_p->page_size, &(bpth->tuple_definitions));
					inserted = 1;

					// release lock on both: new and curr page
					dam_p->release_writer_lock_on_page(dam_p->context, new_page);
					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

					// if there are locked parents, we need to propogate the split
					curr_page = (void*) get_back_of_arraylist(&locked_parents);
					pop_back_from_arraylist(&locked_parents);
				}
				break;
			}
			case INTERIOR_PAGE_TYPE :
			{
				// we are looping forwards to search for a leaf page to insert this record
				if(parent_index_insert == NULL && !inserted)
				{
					// if the curr page can handle an insert without a split
					// then release locks on all the locked_parents that were locked until now
					// since we won't have to propogate the split
					if(can_accomodate_new_index_entry_without_split_interior_page(curr_page, dam_p->page_size, &(bpth->tuple_definitions)))
					{
						// we need a lock on the handle only if we could be splitting the root
						if(is_handle_locked)
						{
							write_unlock(&(bpth->handle_lock));
							is_handle_locked = 0;
						}

						while(!is_empty_arraylist(&locked_parents))
						{
							void* some_parent = (void*) get_front_of_arraylist(&locked_parents);
							dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
							pop_front_from_arraylist(&locked_parents);
						}
					}

					// search appropriate indirection page_id from curr_page
					int32_t next_indirection_index = find_in_interior_page(curr_page, dam_p->page_size, record, &(bpth->tuple_definitions));
					uint32_t next_page_id = get_index_page_id_from_interior_page(curr_page, dam_p->page_size, next_indirection_index, &(bpth->tuple_definitions));
					
					// lock this next page
					void* next_page = dam_p->acquire_page_with_writer_lock(dam_p->context, next_page_id);

					// now curr_page is a locked parent of this next_page
					push_back_to_arraylist(&locked_parents, curr_page);
					curr_page = next_page;
				}
				// we are looping backwards to propogate the split
				else if(parent_index_insert != NULL && inserted)
				{
					// try to insert record to the curr_page
					int parent_index_inserted = insert_to_sorted_packed_page(curr_page, dam_p->page_size, bpth->tuple_definitions.key_def, bpth->tuple_definitions.index_def, parent_index_insert, NULL);

					// if insert succeeds, then unlock all pages and quit the loop
					if(parent_index_inserted)
					{
						free(parent_index_insert);
						parent_index_insert = NULL;

						while(!is_empty_arraylist(&locked_parents))
						{
							void* some_parent = (void*) get_front_of_arraylist(&locked_parents);
							dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
							pop_front_from_arraylist(&locked_parents);
						}

						dam_p->release_writer_lock_on_page(dam_p->context, curr_page);
						curr_page = NULL;
					}
					// else split this interior node to insert
					else
					{
						// get new blank page, to split this page into
						uint32_t new_page_id;
						void* new_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_page_id);

						// split_insert to this node
						void* next_parent_index_insert = split_insert_interior_page(curr_page, parent_index_insert, new_page, new_page_id, dam_p->page_size, &(bpth->tuple_definitions));
						free(parent_index_insert);
						parent_index_insert = next_parent_index_insert;

						// release lock on both: new and curr page
						dam_p->release_writer_lock_on_page(dam_p->context, new_page);
						dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

						// pop a curr_page (getting immediate parent) to propogate the split
						curr_page = (void*) get_back_of_arraylist(&locked_parents);
						pop_back_from_arraylist(&locked_parents);
					}
				}
				break;
			}
		}
	}

	// need to insert root to this bplus tree
	if(parent_index_insert != NULL) // && is_handle_locked => the handle lock condition will always be satisfied
	{
		insert_new_root_node_HANDLE_UNSAFE(bpth, parent_index_insert, dam_p);

		free(parent_index_insert);
		parent_index_insert = NULL;
	}

	// before quit check if the bplus tree handle is locked
	if(is_handle_locked)
	{
		write_unlock(&(bpth->handle_lock));
		is_handle_locked = 0;
	}

	deinitialize_arraylist(&locked_parents);

	return inserted;
}

int delete_in_bplus_tree(bplus_tree_handle* bpth, const void* key, const data_access_methods* dam_p)
{
	arraylist locked_parents;
	initialize_arraylist(&locked_parents, 64);

	write_lock(&(bpth->handle_lock));
	int is_handle_locked = 1;

	void* curr_page = NULL;
	if(bpth->root_id != NULL_PAGE_REF)
		curr_page = dam_p->acquire_page_with_writer_lock(dam_p->context, bpth->root_id);

	// if the curr_page i.e. the root page is a leaf page OR has more than one index entry, then release handle lock
	// i.e we will not have to remove the root node
	if(get_page_type(curr_page) == LEAF_PAGE_TYPE || get_tuple_count(curr_page) > 1)
	{
		write_unlock(&(bpth->handle_lock));
		is_handle_locked = 0;
	}

	int deleted = 0;

	int delete_parent_index_entry = 0;
	int32_t delete_parent_index_entry_at_index = 0;

	// quit when no locks are held
	while( ! (is_empty_arraylist(&locked_parents) && curr_page == NULL) )
	{
		switch(get_page_type(curr_page))
		{
			case LEAF_PAGE_TYPE :
			{
				// no duplicates, a record must be found to be deleted
				uint16_t found_index;
				int found = search_in_sorted_packed_page(curr_page, dam_p->page_size, bpth->tuple_definitions.key_def, bpth->tuple_definitions.record_def, key, &found_index);
				
				// delete the only record with the given key
				if(found)
					deleted = delete_in_sorted_packed_page(curr_page, dam_p->page_size, bpth->tuple_definitions.record_def, found_index);

				delete_parent_index_entry = 0;
				
				// if this delete made the page lesser than half full, we might have to merge
				if(deleted && is_page_lesser_than_half_full(curr_page, dam_p->page_size, bpth->tuple_definitions.record_def))
				{
					void* parent_page = (void*) get_back_of_arraylist(&locked_parents);
					if(parent_page != NULL)
					{
						// index of the current page in the parent_page's indirection indexes
						int32_t curr_index = find_in_interior_page(parent_page, dam_p->page_size, key, &(bpth->tuple_definitions));

						int32_t next_sibbling_index = curr_index + 1;
						int merge_success_with_next = 0;

						if(next_sibbling_index < get_tuple_count(parent_page))
						{
							// this is the exact parent index entry that we would have to delete, if the merge succeeds
							delete_parent_index_entry_at_index = next_sibbling_index;
							const void* parent_index_record = get_nth_tuple(parent_page, dam_p->page_size, bpth->tuple_definitions.index_def, delete_parent_index_entry_at_index);

							// get sibling page id and lock it
							uint32_t next_sibbling_page_id = get_index_page_id_from_interior_page(parent_page, dam_p->page_size, next_sibbling_index, &(bpth->tuple_definitions));
							void* next_sibbling_page = dam_p->acquire_page_with_writer_lock(dam_p->context, next_sibbling_page_id);

							// try to merge, and if merge mark the parent index entry that we need to delete in the next iteration
							delete_parent_index_entry = merge_leaf_pages(curr_page, parent_index_record, next_sibbling_page, dam_p->page_size, &(bpth->tuple_definitions));
							if(delete_parent_index_entry)
								merge_success_with_next = 1;

							if(delete_parent_index_entry)	// release lock and free the next sibling page
								dam_p->release_writer_lock_and_free_page(dam_p->context, next_sibbling_page);
							else 							// else just release lock
								dam_p->release_writer_lock_on_page(dam_p->context, next_sibbling_page);
						}

						int32_t prev_sibbling_index = curr_index - 1;

						if(!merge_success_with_next && (prev_sibbling_index >= -1))
						{
							// release lock on curr page, since we have to reorder taking the lock only for leaf page
							dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

							// this is the exact parent index entry that we would have to delete, if the merge succeeds
							delete_parent_index_entry_at_index = curr_index;
							const void* parent_index_record = get_nth_tuple(parent_page, dam_p->page_size, bpth->tuple_definitions.index_def, delete_parent_index_entry_at_index);

							// get prev sibling page id and lock it
							uint32_t prev_sibbling_page_id = get_index_page_id_from_interior_page(parent_page, dam_p->page_size, prev_sibbling_index, &(bpth->tuple_definitions));
							void* prev_sibbling_page = dam_p->acquire_page_with_writer_lock(dam_p->context, prev_sibbling_page_id);

							// get curr page id and lock it
							uint32_t curr_page_id = get_index_page_id_from_interior_page(parent_page, dam_p->page_size, curr_index, &(bpth->tuple_definitions));
							curr_page = dam_p->acquire_page_with_writer_lock(dam_p->context, curr_page_id);

							// try to merge, and if merge mark the parent index entry that we need to delete in the next iteration
							delete_parent_index_entry = merge_leaf_pages(prev_sibbling_page, parent_index_record, curr_page, dam_p->page_size, &(bpth->tuple_definitions));

							if(delete_parent_index_entry)
							{
								// release lock and free the curr page
								dam_p->release_writer_lock_and_free_page(dam_p->context, curr_page);
								// set the curr_page to the previous sibling page of the curr_page
								curr_page = prev_sibbling_page;
							}
							else	// else just release lock
								dam_p->release_writer_lock_on_page(dam_p->context, prev_sibbling_page);
						}
					}
				}

				if(deleted && delete_parent_index_entry) // propogate the merge delete
				{
					// pop a curr_page (getting immediate parent) to propogate the merge
					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

					// shift to parent page to decide if we could merge
					curr_page = (void*) get_back_of_arraylist(&locked_parents);
					pop_back_from_arraylist(&locked_parents);
				}
				else  // exit loop
				{
					// release locks on all parents
					while(!is_empty_arraylist(&locked_parents))
					{
						void* some_parent = (void*) get_front_of_arraylist(&locked_parents);
						dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
						pop_front_from_arraylist(&locked_parents);
					}

					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);
					curr_page = NULL;
				}
				break;
			}
			case INTERIOR_PAGE_TYPE :
			{
				if(!deleted) // page indirection to reach corresponding leaf page
				{
					// search appropriate indirection page_id from curr_page
					int32_t next_indirection_index = find_in_interior_page(curr_page, dam_p->page_size, key, &(bpth->tuple_definitions));
					uint32_t next_page_id = get_index_page_id_from_interior_page(curr_page, dam_p->page_size, next_indirection_index, &(bpth->tuple_definitions));

					// if the curr_page would be more than half full,
					// even if the tuple at next_indirection_index or (next_indirection_index + 1) is removed,
					// then remove locks on all the locked_parents pages
					int32_t next_indirection_index_1 = next_indirection_index + 1;
					if( is_page_more_than_half_full(curr_page, dam_p->page_size, bpth->tuple_definitions.index_def)
						&& ( (next_indirection_index == -1) || is_surely_more_than_half_full_even_after_delete_at_index(curr_page, dam_p->page_size, next_indirection_index, &(bpth->tuple_definitions)) )
						&& ( (next_indirection_index_1 == get_tuple_count(curr_page)) || is_surely_more_than_half_full_even_after_delete_at_index(curr_page, dam_p->page_size, next_indirection_index_1, &(bpth->tuple_definitions)) )
					){
						// we need a lock on the handle only if we could be splitting the root
						if(is_handle_locked)
						{
							write_unlock(&(bpth->handle_lock));
							is_handle_locked = 0;
						}

						while(!is_empty_arraylist(&locked_parents))
						{
							void* some_parent = (void*) get_front_of_arraylist(&locked_parents);
							dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
							pop_front_from_arraylist(&locked_parents);
						}
					}

					// lock this next page
					void* next_page = dam_p->acquire_page_with_writer_lock(dam_p->context, next_page_id);

					// now curr_page is a locked parent of this next_page
					push_back_to_arraylist(&locked_parents, curr_page);
					curr_page = next_page;
				}
				else if(deleted && delete_parent_index_entry) // handling merges
				{
					// perform delete as the conditions suggest
					int parent_index_deleted = delete_in_sorted_packed_page(curr_page, dam_p->page_size, bpth->tuple_definitions.index_def, delete_parent_index_entry_at_index);

					delete_parent_index_entry = 0;

					// if this delete made the page lesser than half full, we might have to merge
					if(parent_index_deleted && is_page_lesser_than_half_full(curr_page, dam_p->page_size, bpth->tuple_definitions.index_def))
					{
						void* parent_page = (void*) get_back_of_arraylist(&locked_parents);
						if(parent_page != NULL)
						{
							// index of the current page in the parent_page's indirection indexes
							int32_t curr_index = find_in_interior_page(parent_page, dam_p->page_size, key, &(bpth->tuple_definitions));


							// due to lock contention issues with read threads, we can only merge with next siblings
							int32_t next_sibbling_index = curr_index + 1;
							int merge_success_with_next = 0;
							
							// perform merge of curr_page with next_page
							if(next_sibbling_index < get_tuple_count(parent_page))
							{
								delete_parent_index_entry_at_index = next_sibbling_index;
								const void* parent_index_record = get_nth_tuple(parent_page, dam_p->page_size, bpth->tuple_definitions.index_def, delete_parent_index_entry_at_index);

								uint32_t next_sibbling_page_id = get_index_page_id_from_interior_page(parent_page, dam_p->page_size, next_sibbling_index, &(bpth->tuple_definitions));
								void* next_sibbling_page = dam_p->acquire_page_with_writer_lock(dam_p->context, next_sibbling_page_id);

								// merge with next sibling
								delete_parent_index_entry = merge_interior_pages(curr_page, parent_index_record, next_sibbling_page, dam_p->page_size, &(bpth->tuple_definitions));
								if(delete_parent_index_entry)
									merge_success_with_next = 1;

								if(delete_parent_index_entry)	// release and free the next sibling page
									dam_p->release_writer_lock_and_free_page(dam_p->context, next_sibbling_page);
								else							// else just release lock
									dam_p->release_writer_lock_on_page(dam_p->context, next_sibbling_page);
							}


							// due to lock contention issues with read threads, we can only merge with next siblings
							int32_t prev_sibbling_index = curr_index - 1;

							// if unsuccessfull perform merge of curr_page with prev_page
							if(!merge_success_with_next && (prev_sibbling_index >= -1))
							{
								delete_parent_index_entry_at_index = curr_index;
								const void* parent_index_record = get_nth_tuple(parent_page, dam_p->page_size, bpth->tuple_definitions.index_def, delete_parent_index_entry_at_index);

								uint32_t prev_sibbling_page_id = get_index_page_id_from_interior_page(parent_page, dam_p->page_size, prev_sibbling_index, &(bpth->tuple_definitions));
								void* prev_sibbling_page = dam_p->acquire_page_with_writer_lock(dam_p->context, prev_sibbling_page_id);

								// merge with prev sibling
								delete_parent_index_entry = merge_interior_pages(prev_sibbling_page, parent_index_record, curr_page, dam_p->page_size, &(bpth->tuple_definitions));

								if(delete_parent_index_entry)
								{
									// release lock and free curr_page
									dam_p->release_writer_lock_and_free_page(dam_p->context, curr_page);
									// set the curr_page to the previous sibling page of the curr_page
									curr_page = prev_sibbling_page;
								}
								else	// else just release lock
									dam_p->release_writer_lock_on_page(dam_p->context, prev_sibbling_page);
							}
						}
					}

					if(!delete_parent_index_entry) // exit loop
					{
						// release locks on all parents
						while(!is_empty_arraylist(&locked_parents))
						{
							void* some_parent = (void*) get_front_of_arraylist(&locked_parents);
							dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
							pop_front_from_arraylist(&locked_parents);
						}

						dam_p->release_writer_lock_on_page(dam_p->context, curr_page);
						curr_page = NULL;
					}
					else
					{
						// pop a curr_page (getting immediate parent) to propogate the merge
						dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

						// shift to parent page to decide if we could merge
						curr_page = (void*) get_back_of_arraylist(&locked_parents);
						pop_back_from_arraylist(&locked_parents);
					}
				}
				break;
			}
		}
	}

	if(is_handle_locked)
	{
		void* root_page = dam_p->acquire_page_with_writer_lock(dam_p->context, bpth->root_id);

		// remove bplus tree root, only if it is empty
		if(get_tuple_count(root_page) == 0)
		{
			if(get_page_type(root_page) == INTERIOR_PAGE_TYPE)
				// the new root is at the ALL_LEAST_REF of the current interior root page
				bpth->root_id = get_index_page_id_from_interior_page(root_page, dam_p->page_size, -1, &(bpth->tuple_definitions));
			else	// if the empty root node is LEAF_PAGE_TYPE, the bplus tree is empty
				bpth->root_id = NULL_PAGE_REF;

			dam_p->release_writer_lock_and_free_page(dam_p->context, root_page);
		}
		else
			dam_p->release_writer_lock_on_page(dam_p->context, root_page);
	}

	// before quit check if the bplus tree handle is locked
	if(is_handle_locked)
	{
		write_unlock(&(bpth->handle_lock));
		is_handle_locked = 0;
	}

	deinitialize_arraylist(&locked_parents);

	return deleted;
}

static void print_bplus_tree_sub_tree(uint32_t root_id, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	void* curr_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root_id);

	switch(get_page_type(curr_page))
	{
		case LEAF_PAGE_TYPE :
		{
			printf("LEAF page_id = %u\n", root_id);
			print_page(curr_page, dam_p->page_size, bpttds->record_def);
			break;
		}
		case INTERIOR_PAGE_TYPE :
		{
			// call print on all its children
			for(int32_t i = -1; i < get_tuple_count(curr_page); i++)
			{
				uint32_t child_id = get_index_page_id_from_interior_page(curr_page, dam_p->page_size, i, bpttds);
				print_bplus_tree_sub_tree(child_id, bpttds, dam_p);
			}

			// call print on itself
			printf("INTR page_id = %u\n", root_id);
			print_page(curr_page, dam_p->page_size, bpttds->index_def);

			break;
		}
	}

	dam_p->release_reader_lock_on_page(dam_p->context, curr_page);
}

void print_bplus_tree(bplus_tree_handle* bpth, const data_access_methods* dam_p)
{
	read_lock(&(bpth->handle_lock));

	if(bpth->root_id != NULL_PAGE_REF)
		print_bplus_tree_sub_tree(bpth->root_id, &(bpth->tuple_definitions), dam_p);
	else
		printf("EMPTY BPLUS_TREE\n");

	read_unlock(&(bpth->handle_lock));
}