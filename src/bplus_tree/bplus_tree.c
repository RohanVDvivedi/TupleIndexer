#include<bplus_tree.h>

#include<locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<persistent_page_functions.h>
#include<tuple.h>

#include<stdlib.h>

uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* dam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page root_page = get_new_persistent_page_with_write_lock(dam_p, transaction_id, abort_error);

	// failure to acquire a new page
	if(*abort_error)
		return bpttd_p->pas_p->NULL_PAGE_ID;

	// if init_page fails
	init_bplus_tree_leaf_page(&root_page, bpttd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(dam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
		return bpttd_p->pas_p->NULL_PAGE_ID;
	}

	uint64_t res = root_page.page_id;
	release_lock_on_persistent_page(dam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
	if(*abort_error)
		return bpttd_p->pas_p->NULL_PAGE_ID;

	return res;
}

int destroy_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
	// create a stack
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	{
		// get lock on the root page of the bplus_tree
		persistent_page root_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, READ_LOCK, abort_error);
		if(*abort_error)
			return 0;

		// pre cache level of the root_page
		uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// edge case : if the root itself is a leaf page then free it, no locked_pages_stack required
		if(is_bplus_tree_leaf_page(&root_page, bpttd_p))
		{
			release_lock_on_persistent_page(dam_p, transaction_id, &root_page, FREE_PAGE, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(dam_p, transaction_id, &root_page, NONE_OPTION, abort_error);
				return 0;
			}
			return 1;
		}

		// create a stack of capacity = levels
		initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page));
	}

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// if this is an interior page whose all child pages are leaf pages, i.e. level = 1
		if(get_level_of_bplus_tree_page(&(curr_locked_page->ppage), bpttd_p) == 1)
		{
			// free all child leaf pages of this page
			// without acquiring lock on this pages

			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

			// free the child leaf page id at index -1
			uint64_t child_leaf_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), -1, bpttd_p);
			free_persistent_page(dam_p, transaction_id, child_leaf_page_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// free the child leaf page id at index [0, tuple_count)
			for(uint32_t i = 0; i < tuple_count; i++)
			{
				child_leaf_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), i, bpttd_p);
				free_persistent_page(dam_p, transaction_id, child_leaf_page_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}

			// free it and pop it from the stack
			release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page->ppage), FREE_PAGE, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			pop_from_locked_pages_stack(locked_pages_stack_p);
		}
		// handle free-ing on pages at level >= 2
		else
		{
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

			// if child index is -1 or lesser than tuple_count
			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count)
			{
				// then push it's child at child_index onto the stack (with child_index = -1), while incrementing its child index
				uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index++, bpttd_p);
				persistent_page child_page = acquire_persistent_page_with_lock(dam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page));
			}
			else // we have freed all its children, now we can free this page
			{
				// free it and pop it from the stack
				release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page->ppage), FREE_PAGE, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
				pop_from_locked_pages_stack(locked_pages_stack_p);
			}
		}
	}

	ABORT_ERROR:;
	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);

	return 1;
}

void print_bplus_tree(uint64_t root_page_id, int only_leaf_pages, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
	// print the root page id of the bplsu tree
	printf("\n\nBplus_tree @ root_page_id = %"PRIu64"\n\n", root_page_id);

	// create a stack
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	{
		// get lock on the root page of the bplus_tree
		persistent_page root_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, READ_LOCK, abort_error);
		if(*abort_error)
			return;

		// pre cache level of the root_page
		uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// create a stack of capacity = levels
		initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page));
	}

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// print current page as a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
		{
			// print this page and its page_id
			printf("page_id : %"PRIu64"\n\n", curr_locked_page->ppage.page_id);
			print_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p);
			printf("xxxxxxxxxxxxx\n\n");

			// unlock it and pop it from the stack
			release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
			pop_from_locked_pages_stack(locked_pages_stack_p);
			if(*abort_error)
				goto ABORT_ERROR;
		}
		// print current page as an interior page
		else
		{
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

			// if child index is -1 or lesser than tuple_count
			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count)
			{
				// then push it's child at child_index onto the stack (with child_index = -1), while incrementing its child index
				uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index++, bpttd_p);
				persistent_page child_page = acquire_persistent_page_with_lock(dam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page));
			}
			else // we have printed all its children, now we print this page
			{
				// here the child_index of the page is tuple_count

				if(!only_leaf_pages)
				{
					// print this page and its page_id
					printf("page_id : %"PRIu64"\n\n", curr_locked_page->ppage.page_id);
					print_bplus_tree_interior_page(&(curr_locked_page->ppage), bpttd_p);
					printf("xxxxxxxxxxxxx\n");
				}

				// pop it from the stack and unlock it
				release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
				pop_from_locked_pages_stack(locked_pages_stack_p);
				if(*abort_error)
					goto ABORT_ERROR;
			}
		}
	}

	ABORT_ERROR:;
	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);
}