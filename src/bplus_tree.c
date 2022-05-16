#include<bplus_tree.h>

#include<bplus_tree_locked_pages_stack.h>

#include<arraylist.h>

uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

bplus_tree_cursor* find_in_bplus_tree(uint64_t root_page_id, const void* key, int scan_dir, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	locked_page_info* curr_locked_page = lock_page_and_get_new_locked_page_info(root_page_id, 1, 1, bpttds, dam_p);

	// create a stack of capacity = levels + 3 
	arraylist locked_pages_stack;
	initialize_arraylist(&locked_pages_stack, curr_locked_page->level + 4);

	// push the root page onto the stack
	push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, curr_locked_page);

	// found will be set if a record with the key is found in the bplus_tree
	int found = 0;
	// inserted will be set if the record, was inserted
	int inserted = 0;

	// a tuple that needs to be inserted in to the parent page
	// this happens upon a split
	const void* parent_insert = NULL;

	while(!is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		if(!inserted && !found)
		// go deeper towards the leaf of the tree, pushing curr_locked_page's child on the stack
		{

		}
		else
		// parent_insert needs to be inserted in to this page and we need to pop curr_locked_page
		{

		}
	}

	deinitialize_arraylist(&locked_pages_stack);

	return inserted;
}

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

int destroy_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

void print_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);