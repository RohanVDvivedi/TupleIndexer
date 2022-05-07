#include<bplus_tree_locked_pages_stack.h>

#include<stdlib.h>

#include<bplus_tree_page.h>

locked_page_info* lock_page_and_get_new_locked_page_info(uint64_t page_id, int get_write_lock, int is_root, data_access_methods* dam_p)
{
	void* page = NULL;

	if(get_write_lock)
		page = dam_p->acquire_page_with_writer_lock(dam_p->context, page_id);
	else
		page = dam_p->acquire_page_with_reader_lock(dam_p->context, page_id);

	// a failure
	if(page == NULL)
		return NULL;

	locked_page_info* lpi_p = malloc(sizeof(locked_page_info));
	lpi_p->page_id = page_id;
	lpi_p->page = page;
	lpi_p->level = get_level_of_bplus_tree_page(page, dam_p->page_size);
	lpi_p->is_write_locked = get_write_lock;
	lpi_p->is_root = is_root;

	// this field can not be initialized with the minimal information that we have
	lpi_p->child_index = -1;

	return lpi_p;
}

int unlock_page_and_delete_locked_page_info(locked_page_info* lpi_p, int should_free_this_page, data_access_methods* dam_p)
{
	int return_val = 0;

	if(should_free_this_page)
	{
		if(lpi_p->is_write_locked)
			return_val = dam_p->release_writer_lock_on_page(dam_p->context, lpi_p->page);
		else
			return_val = dam_p->release_reader_lock_on_page(dam_p->context, lpi_p->page);
	}
	else
	{
		if(lpi_p->is_write_locked)
			return_val = dam_p->release_writer_lock_and_free_page(dam_p->context, lpi_p->page);
		else
			return_val = dam_p->release_reader_lock_and_free_page(dam_p->context, lpi_p->page);
	}

	free(lpi_p);

	return return_val;
}

void push_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p, locked_page_info* lpi_p)
{

}

locked_page_info* pop_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p)
{

}

locked_page_info* get_top_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p)
{

}

void fifo_unlock_all_bplus_tree_locked_pages_stack(arraylist* btlps_p, data_access_methods* dam_p)
{

}