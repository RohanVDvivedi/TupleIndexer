#include<bplus_tree.h>

#include<locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

#include<stdlib.h>

uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	uint64_t root_page_id;
	void* root_page = dam_p->get_new_page_with_write_lock(dam_p->context, &root_page_id);

	// if init_page fails
	if(!init_bplus_tree_leaf_page(root_page, bpttd_p))
	{
		dam_p->release_writer_lock_and_free_page(dam_p->context, root_page);
		return bpttd_p->NULL_PAGE_ID;
	}

	dam_p->release_writer_lock_on_page(dam_p->context, root_page, 1);
	return root_page_id;
}

int destroy_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	void* root_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root_page_id);

	// pre cache level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(root_page, bpttd_p);

	// edge case : if the root itself is a leaf page then free it, no locked_pages_stack required
	if(is_bplus_tree_leaf_page(root_page, bpttd_p))
	{
		dam_p->release_reader_lock_and_free_page(dam_p->context, root_page);
		return 1;
	}

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});
	initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, root_page_id));

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// if this is an interior page whose all child pages are leaf pages, i.e. level = 1
		if(get_level_of_bplus_tree_page(curr_locked_page->ppage.page, bpttd_p) == 1)
		{
			// free all child leaf pages of this page
			// without acquiring lock on this pages

			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_page(curr_locked_page->ppage.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

			// free the child leaf page id at index -1
			uint64_t child_leaf_page_id = find_child_page_id_by_child_index(curr_locked_page->ppage.page, -1, bpttd_p);;
			dam_p->free_page(dam_p->context, child_leaf_page_id);

			// free the child leaf page id at index [0, tuple_count)
			for(uint32_t i = 0; i < tuple_count; i++)
			{
				child_leaf_page_id = find_child_page_id_by_child_index(curr_locked_page->ppage.page, i, bpttd_p);;
				dam_p->free_page(dam_p->context, child_leaf_page_id);
			}

			// free it and pop it from the stack
			dam_p->release_reader_lock_and_free_page(dam_p->context, curr_locked_page->ppage.page);
			pop_from_locked_pages_stack(locked_pages_stack_p);
		}
		// handle free-ing on pages at level >= 2
		else
		{
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_page(curr_locked_page->ppage.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

			// if child index is -1 or lesser than tuple_count
			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count)
			{
				// then push it's child at child_index onto the stack (with child_index = -1), while incrementing its child index
				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->ppage.page, curr_locked_page->child_index++, bpttd_p);
				void* child_page = dam_p->acquire_page_with_reader_lock(dam_p->context, child_page_id);

				push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, child_page_id));
			}
			else // we have free all its children, now we free this page
			{
				// free it and pop it from the stack
				dam_p->release_reader_lock_and_free_page(dam_p->context, curr_locked_page->ppage.page);
				pop_from_locked_pages_stack(locked_pages_stack_p);
			}
		}
	}

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		dam_p->release_reader_lock_on_page(dam_p->context, bottom->ppage.page);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);

	return 1;
}

void print_bplus_tree(uint64_t root_page_id, int only_leaf_pages, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// print the root page id of the bplsu tree
	printf("\n\nBplus_tree @ root_page_id = %"PRIu64"\n\n", root_page_id);

	// get lock on the root page of the bplus_tree
	void* root_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root_page_id);

	// pre cache level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(root_page, bpttd_p);

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});
	initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, root_page_id));

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// print current page as a leaf page
		if(is_bplus_tree_leaf_page(curr_locked_page->ppage.page, bpttd_p))
		{
			// print this page and its page_id
			printf("page_id : %"PRIu64"\n\n", curr_locked_page->ppage.page_id);
			print_bplus_tree_leaf_page(curr_locked_page->ppage.page, bpttd_p);
			printf("xxxxxxxxxxxxx\n\n");

			// unlock it and pop it from the stack
			dam_p->release_reader_lock_on_page(dam_p->context, curr_locked_page->ppage.page);
			pop_from_locked_pages_stack(locked_pages_stack_p);
		}
		// print current page as an interior page
		else
		{
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_page(curr_locked_page->ppage.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

			// if child index is -1 or lesser than tuple_count
			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count)
			{
				// then push it's child at child_index onto the stack (with child_index = -1), while incrementing its child index
				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->ppage.page, curr_locked_page->child_index++, bpttd_p);
				void* child_page = dam_p->acquire_page_with_reader_lock(dam_p->context, child_page_id);

				push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, child_page_id));
			}
			else // we have printed all its children, now we print this page
			{
				// here the child_index of the page is tuple_count

				if(!only_leaf_pages)
				{
					// print this page and its page_id
					printf("page_id : %"PRIu64"\n\n", curr_locked_page->ppage.page_id);
					print_bplus_tree_interior_page(curr_locked_page->ppage.page, bpttd_p);
					printf("xxxxxxxxxxxxx\n");
				}

				// pop it from the stack and unlock it
				dam_p->release_reader_lock_on_page(dam_p->context, curr_locked_page->ppage.page);
				pop_from_locked_pages_stack(locked_pages_stack_p);
			}
		}
	}

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		dam_p->release_reader_lock_on_page(dam_p->context, bottom->ppage.page);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);
}