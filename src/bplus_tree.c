#include<bplus_tree.h>

#include<bplus_tree_locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<sorted_packed_page_util.h>

#include<arraylist.h>

uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

bplus_tree_cursor* find_in_bplus_tree(uint64_t root_page_id, const void* key, int scan_dir, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	locked_page_info* curr_locked_page = lock_page_and_get_new_locked_page_info(root_page_id, 1, 1, bpttd_p, dam_p);

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

		if(!inserted && !found) // go deeper towards the leaf of the tree, pushing curr_locked_page's child on the stack
		{
			if(curr_locked_page->level == 0) // is a leaf page, insert / split_insert to the leaf page
			{
				// check if the record already exists in this leaf page
				found = (NO_TUPLE_FOUND != find_first_in_sorted_packed_page(
									curr_locked_page->page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									record, bpttd_p->record_def, bpttd_p->key_element_ids
								));

				// if it does not exist then try to insert it
				if(!found)
				{
					uint32_t insertion_point;

					inserted = insert_to_sorted_packed_page(
									curr_locked_page->page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									record, 
									&insertion_point
								);

					// if the insertion fails
					if(!inserted)
					{
						// then compact the page and then try to insert it

						// if it still fails then call the split insert
					}
				}

				// found or the record was inserted without requiring a split
				if(found || (inserted && parent_insert == NULL))
				{
					// the insert operation is completed
					fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);
				}
				else // the split needs to be propogated to the parent pages
				{
					pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

					unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);
				}
			}
			else // is an interior page, grab its child and push it to the tree
			{
				// figure out which child page to go to next
				curr_locked_page->child_index = find_child_index_for_record(curr_locked_page->page, record, bpttd_p);

				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, child_index, bpttd_p);

				// get lock on the next child page (this page is surely not the root page)
				locked_page_info* child_locked_page = lock_page_and_get_new_locked_page_info(child_page_id, 1, 0, bpttds, dam_p);

				// if it will not require a split, then release locks on all the parent pages
				if( (child_locked_page->level == 0 && !may_require_split_for_insert_for_bplus_tree(child_locked_page->page, bpttd_p->page_size, bpttd_p->record_def))
				||  (child_locked_page->level  > 0 && !may_require_split_for_insert_for_bplus_tree(child_locked_page->page, bpttd_p->page_size, bpttd_p->index_def )) )
				{
					fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);
				}

				// push this child page onto the stack
				push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, child_locked_page);
			}
		}
		else // parent_insert needs to be inserted in to this page and we need to pop curr_locked_page
		{
			// here (curr_locked_page->level > 0) is a MUST condition
		}
	}

	deinitialize_arraylist(&locked_pages_stack);

	return inserted;
}

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

int destroy_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

void print_bplus_tree_recursively(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);