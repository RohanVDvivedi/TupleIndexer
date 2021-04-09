#include<bplus_tree.h>

#include<sorted_packed_page_util.h>

#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>

#include<storage_capacity_page_util.h>

#include<arraylist.h>

#include<stdlib.h>
#include<string.h>

int create_new_bplus_tree(bplus_tree_handle* bpth, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	initialize_rwlock(&(bpth->handle_lock));

	write_lock(&(bpth->handle_lock));

	void* root_page = dam_p->get_new_page_with_write_lock(dam_p->context, &(bpth->root_id));

	// getting new page to write failed
	if(root_page == NULL)
	{
		write_unlock(&(bpth->handle_lock));
		return 0;
	}

	write_unlock(&(bpth->handle_lock));

	init_leaf_page(root_page, dam_p->page_size, bpttds);
	
	dam_p->release_writer_lock_on_page(dam_p->context, root_page);
	
	return 1;
}

const void* find_in_bplus_tree(bplus_tree_handle* bpth, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	void* record_found = NULL;

	read_lock(&(bpth->handle_lock));

	void* curr_page = dam_p->acquire_page_with_reader_lock(dam_p->context, bpth->root_id);

	read_unlock(&(bpth->handle_lock));

	while(curr_page != NULL)
	{
		switch(get_page_type(curr_page))
		{
			case LEAF_PAGE_TYPE :
			{
				// find record for the given key in b+tree
				uint16_t found_index;
				int found = search_in_sorted_packed_page(curr_page, dam_p->page_size, bpttds->key_def, bpttds->record_def, key, &found_index);

				if(found) // then, copy it to record_found
				{
					const void* record_found_original = get_nth_tuple(curr_page, dam_p->page_size, bpttds->record_def, found_index);
					uint32_t record_found_original_size = get_tuple_size(bpttds->record_def, record_found_original);

					record_found = malloc(record_found_original_size);
					memmove(record_found, record_found_original, record_found_original_size);
				}

				// release lock and cleanup
				dam_p->release_reader_lock_on_page(dam_p->context, curr_page);
				curr_page = NULL;
				break;
			}
			case INTERIOR_PAGE_TYPE :
			{
				// find next page_id to jump to the next level in the b+tree
				int32_t next_indirection_index = find_in_interior_page(curr_page, dam_p->page_size, key, bpttds);
				uint32_t next_page_id = get_index_page_id_from_interior_page(curr_page, dam_p->page_size, next_indirection_index, bpttds);

				// lock next page, prior to releasing the lock on the current page
				void* next_page = dam_p->acquire_page_with_reader_lock(dam_p->context, next_page_id);
				dam_p->release_reader_lock_on_page(dam_p->context, curr_page);

				// reiterate until you reach the leaf page
				curr_page = next_page;
				break;
			}
		}
	}

	return record_found;
}

static void insert_new_root_node_HANDLE_UNSAFE(bplus_tree_handle* bpth, const void* parent_index_insert, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	uint32_t old_root_id = bpth->root_id;
	
	uint32_t new_root_page_id;
	void* new_root_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_root_page_id);
	init_interior_page(new_root_page, dam_p->page_size, bpttds);

	// insert the only entry for a new root level
	insert_to_sorted_packed_page(new_root_page, dam_p->page_size, bpttds->key_def, bpttds->index_def, parent_index_insert, NULL);

	// update all least referenc of this is page
	set_index_page_id_in_interior_page(new_root_page, dam_p->page_size, -1, bpttds, old_root_id);

	dam_p->release_writer_lock_on_page(dam_p->context, new_root_page);

	bpth->root_id = new_root_page_id;
}

int insert_in_bplus_tree(bplus_tree_handle* bpth, const void* record, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	// record size must be lesser than or equal to half the allotted page size
	// or even generated index entry size must be lesser than or equal to half the allotted page size
	uint32_t record_size_on_page = get_tuple_size(bpttds->record_def, record) + get_additional_space_occupied_per_tuple(dam_p->page_size, bpttds->record_def);
	uint32_t index_entry_size_on_page = (get_tuple_size(bpttds->key_def, record) + 4) + get_additional_space_occupied_per_tuple(dam_p->page_size, bpttds->record_def);
	uint32_t max_tuple_size_on_page = get_space_to_be_allotted_for_all_tuples(1, dam_p->page_size, bpttds->record_def) / 2;
	if(record_size_on_page > max_tuple_size_on_page || index_entry_size_on_page > max_tuple_size_on_page)
		return 0;

	arraylist locked_parents;
	initialize_arraylist(&locked_parents, 64);

	write_lock(&(bpth->handle_lock));
	int is_handle_locked = 1;

	void* curr_page = dam_p->acquire_page_with_writer_lock(dam_p->context, bpth->root_id);

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
				int found = search_in_sorted_packed_page(curr_page, dam_p->page_size, bpttds->key_def, bpttds->record_def, record, NULL);

				// try to insert record to the curr_page, only if the key is not found on the page
				if(!found)
					inserted = insert_to_sorted_packed_page(curr_page, dam_p->page_size, bpttds->key_def, bpttds->record_def, record, NULL);
				
				// if inserted or found, then unlock this page and all the parent pages
				if(found || inserted)
				{
					while(!is_empty_arraylist(&locked_parents))
					{
						void* some_parent = (void*) get_front(&locked_parents);
						dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
						pop_front(&locked_parents);
					}

					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);
					curr_page = NULL;
				}
				// insert forcefully with a split
				else if(!found)
				{
					// get new blank page, to split this page into
					uint32_t new_page_id;
					void* new_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_page_id);
					
					// split and insert to this leaf node and propogate the parent_index_insert, don not forget to set the inserted flag
					parent_index_insert = split_insert_leaf_page(curr_page, record, new_page, new_page_id, dam_p->page_size, bpttds);
					inserted = 1;

					// release lock on both: new and curr page
					dam_p->release_writer_lock_on_page(dam_p->context, new_page);
					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

					// if there are locked parents, we need to propogate the split
					curr_page = (void*) get_back(&locked_parents);
					pop_back(&locked_parents);
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
					if(is_page_lesser_than_or_equal_to_half_full(curr_page, dam_p->page_size, bpttds->index_def))
					{
						// we need a lock on the handle only if we could be splitting the root
						if(is_handle_locked)
						{
							write_unlock(&(bpth->handle_lock));
							is_handle_locked = 0;
						}

						while(!is_empty_arraylist(&locked_parents))
						{
							void* some_parent = (void*) get_front(&locked_parents);
							dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
							pop_front(&locked_parents);
						}
					}

					// search appropriate indirection page_id from curr_page
					int32_t next_indirection_index = find_in_interior_page(curr_page, dam_p->page_size, record, bpttds);
					uint32_t next_page_id = get_index_page_id_from_interior_page(curr_page, dam_p->page_size, next_indirection_index, bpttds);
					
					// lock this next page
					void* next_page = dam_p->acquire_page_with_writer_lock(dam_p->context, next_page_id);

					// now curr_page is a locked parent of this next_page
					push_back(&locked_parents, curr_page);
					curr_page = next_page;
				}
				// we are looping backwards to propogate the split
				else if(parent_index_insert != NULL && inserted)
				{
					// try to insert record to the curr_page
					int parent_index_inserted = insert_to_sorted_packed_page(curr_page, dam_p->page_size, bpttds->key_def, bpttds->index_def, parent_index_insert, NULL);

					// if insert succeeds, then unlock all pages and quit the loop
					if(parent_index_inserted)
					{
						free(parent_index_insert);
						parent_index_insert = NULL;

						while(!is_empty_arraylist(&locked_parents))
						{
							void* some_parent = (void*) get_front(&locked_parents);
							dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
							pop_front(&locked_parents);
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
						void* next_parent_index_insert = split_insert_interior_page(curr_page, parent_index_insert, new_page, new_page_id, dam_p->page_size, bpttds);
						free(parent_index_insert);
						parent_index_insert = next_parent_index_insert;

						// release lock on both: new and curr page
						dam_p->release_writer_lock_on_page(dam_p->context, new_page);
						dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

						// pop a curr_page (getting immediate parent) to propogate the split
						curr_page = (void*) get_back(&locked_parents);
						pop_back(&locked_parents);
					}
				}
				break;
			}
		}
	}

	// need to insert root to this bplus tree
	if(parent_index_insert != NULL) // && is_handle_locked => the handle lock condition will always be satisfied
	{
		insert_new_root_node_HANDLE_UNSAFE(bpth, parent_index_insert, bpttds, dam_p);

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

int delete_in_bplus_tree(bplus_tree_handle* bpth, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	arraylist locked_parents;
	initialize_arraylist(&locked_parents, 64);

	write_lock(&(bpth->handle_lock));
	int is_handle_locked = 1;

	void* curr_page = dam_p->acquire_page_with_writer_lock(dam_p->context, bpth->root_id);

	// if the curr_page i.e. the root page is a leaf page OR has more than one index entry, then release handle lock
	// i.e we will not have to remove the root node
	if(is_leaf_page(curr_page) || get_index_entry_count_in_interior_page(curr_page) > 1)
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
			{// no duplicates
				// a record must be found to be deleted
				uint16_t found_index;
				int found = search_in_sorted_packed_page(curr_page, dam_p->page_size, bpttds->key_def, bpttds->record_def, key, &found_index);
				
				// delete the only record with the given key
				if(found)
					deleted = delete_in_sorted_packed_page(curr_page, dam_p->page_size, bpttds->record_def, found_index);

				// if this delete made the page lesser than half full, we might have to merge
				if(is_page_lesser_than_half_full(curr_page, dam_p->page_size, bpttds->record_def))
				{
					void* parent_page = (void*) get_back(&locked_parents);
					if(parent_page != NULL)
					{
						// index of the current page in the parent_page's indirection indexes
						int32_t curr_index = find_in_interior_page(parent_page, dam_p->page_size, key, bpttds);

						// due to lock contention issues with read threads, we can only merge with next siblings
						int32_t sibbling_index = curr_index + 1;

						if(sibbling_index < get_index_entry_count_in_interior_page(parent_page))
						{
							// this is the exact parent index entry that we would have to delete, if the merge succeeds
							const void* parent_index_record = get_index_entry_from_interior_page(parent_page, dam_p->page_size, sibbling_index, bpttds);

							// get sibling page id and lock it
							uint32_t sibbling_page_id = get_index_page_id_from_interior_page(parent_page, dam_p->page_size, sibbling_index, bpttds);
							void* sibbling_page = dam_p->acquire_page_with_writer_lock(dam_p->context, sibbling_page_id);

							// try to merge, and if merge mark the parent index entry that we need to delete in the next iteration
							delete_parent_index_entry = merge_leaf_pages(curr_page, parent_index_record, sibbling_page, dam_p->page_size, bpttds);
							if(delete_parent_index_entry)
								delete_parent_index_entry_at_index = sibbling_index;

							// release lock on sibling page
							dam_p->release_writer_lock_on_page(dam_p->context, sibbling_page);
						}
					}
				}

				if(!delete_parent_index_entry) // exit loop
				{
					// release locks on all parents
					while(!is_empty_arraylist(&locked_parents))
					{
						void* some_parent = (void*) get_front(&locked_parents);
						dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
						pop_front(&locked_parents);
					}

					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);
					curr_page = NULL;
				}
				else
				{
					// pop a curr_page (getting immediate parent) to propogate the merge
					dam_p->release_writer_lock_on_page(dam_p->context, curr_page);

					// shift to parent page to decide if we could merge
					curr_page = (void*) get_back(&locked_parents);
					pop_back(&locked_parents);
				}
				break;
			}
			case INTERIOR_PAGE_TYPE :
			{
				if(!deleted) // page indirection to reach corresponding leaf page
				{
					// search appropriate indirection page_id from curr_page
					int32_t next_indirection_index = find_in_interior_page(curr_page, dam_p->page_size, key, bpttds);
					uint32_t next_page_id = get_index_page_id_from_interior_page(curr_page, dam_p->page_size, next_indirection_index, bpttds);

					// if the curr_page would be more than half full,
					// even if the tuple at (next_indirection_index + 1) is removed,
					// then remove locks on all the locked_parents pages
					if(0)
					{
						// we need a lock on the handle only if we could be splitting the root
						if(is_handle_locked)
						{
							write_unlock(&(bpth->handle_lock));
							is_handle_locked = 0;
						}

						while(!is_empty_arraylist(&locked_parents))
						{
							void* some_parent = (void*) get_front(&locked_parents);
							dam_p->release_writer_lock_on_page(dam_p->context, some_parent);
							pop_front(&locked_parents);
						}
					}

					// lock this next page
					void* next_page = dam_p->acquire_page_with_writer_lock(dam_p->context, next_page_id);

					// now curr_page is a locked parent of this next_page
					push_back(&locked_parents, curr_page);
					curr_page = next_page;
				}
				else if(deleted && delete_parent_index_entry_at_index) // handling merges
				{
					// perform delete as the conditions suggest
				}
				break;
			}
		}
	}

	if(delete_parent_index_entry && is_handle_locked)
	{
		// remove bplus tree root, only if it is an interior page
		// we do not remove the last root leaf page
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
			for(int32_t i = -1; i < ((int32_t)(get_index_entry_count_in_interior_page(curr_page))); i++)
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

void print_bplus_tree(bplus_tree_handle* bpth, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	read_lock(&(bpth->handle_lock));

	print_bplus_tree_sub_tree(bpth->root_id, bpttds, dam_p);

	read_unlock(&(bpth->handle_lock));
}