#include<bplus_tree.h>

#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>

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
	void* curr_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root);

	while(curr_page != NULL)
	{
		switch(get_page_type(curr_page))
		{
			case LEAF_PAGE_TYPE :
			{
				// find record for the given key in b+tree

				// release lock and cleanup
				dam_p->release_reader_lock_on_page(dam_p->context, curr_page);
				curr_page = NULL
				break;
			}
			case INTERIOR_PAGE_TYPE :
			{
				// find next page_id to jump to the next level in the b+tree
				int32_t next_indirection_index = find_in_interior_page(curr_page, dam_p->page_size, key, 0, bpttds);
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
}

int insert_in_bplus_tree(uint32_t* root, const void* record, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);

int delete_in_bplus_tree(uint32_t* root, const void* key, const bplus_tree_tuple_defs* bpttds, const data_access_methods* dam_p);
