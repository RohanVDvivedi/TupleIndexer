#include<bplus_tree.h>

#include<sorted_packed_page_util.h>

#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>

#include<arraylist.h>

#include<stdlib.h>
#include<string.h>

uint32_t create_new_bplus_tree(const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	uint32_t root_page_id;
	void* root_page = dam_p->get_new_page_with_write_lock(dam_p->context, &root_page_id);
		if(root_page == NULL)
			return NULL_PAGE_REF;

		init_leaf_page(root_page, dam_p->page_size, bpttds);
	
	dam_p->release_writer_lock_on_page(dam_p->context, root_page);
	
	return root_page_id;
}

const void* find_in_bplus_tree(uint32_t root, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	void* record_found = NULL;

	void* curr_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root);

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

int insert_in_bplus_tree(uint32_t* root, const void* record, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p)
{
	// record.size must be lesser than or equal to half the page size

	arraylist locked_parents;
	initialize_arraylist(&locked_parents, 64);

	void* curr_locked_page = NULL;

	void* parent_index_insert = NULL;

	int inserted = 0;

	int quit_loop = 0;
	while(!quit_loop)
	{
		switch(get_page_type(curr_locked_page))
		{
			case LEAF_PAGE_TYPE :
			{
				// try to insert record to the curr_page
				// if insert succeeds, then unlock all pages and quit the loop
				// else split this leaf node and then insert
				// set the parent_index_insert, and pop a curr_page (getting immediate parent)
				break;
			}
			case INTERIOR_PAGE_TYPE :
			{
				if(parent_index_insert == NULL)
				{
					// if the current page is lesser than half full, then release locks on all the locked_parents
					// search appropriate indirection and lock this next page
					// insert curr_page to this locked_parents queue, and set next_page to curr_page
					// release appropriate locks in the locked_parents queue
				}
				else	// index insert to parent
				{
					// try to insert record to the curr_page
					// if insert succeeds, then unlock all pages and quit the loop
					// else split this interior node and then insert
					// set the parent_index_insert, and pop a curr_page (getting immediate parent)
				}
				break;
			}
		}
	}

	deinitialize_arraylist(&locked_parents);

	return inserted;
}

int delete_in_bplus_tree(uint32_t* root, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);
