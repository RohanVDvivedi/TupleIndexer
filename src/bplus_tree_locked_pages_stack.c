#include<bplus_tree_locked_pages_stack.h>

#include<stdlib.h>

#include<bplus_tree_page.h>

int unlock_locked_page_info(const locked_page_info* lpi_p, int should_free_this_page, int was_modified_if_write_lock, const data_access_methods* dam_p)
{
	if(!should_free_this_page)
	{
		if(lpi_p->is_write_locked)
			return dam_p->release_writer_lock_on_page(dam_p->context, lpi_p->page, was_modified_if_write_lock);
		else
			return dam_p->release_reader_lock_on_page(dam_p->context, lpi_p->page);
	}
	else
	{
		if(lpi_p->is_write_locked)
			return dam_p->release_writer_lock_and_free_page(dam_p->context, lpi_p->page);
		else
			return dam_p->release_reader_lock_and_free_page(dam_p->context, lpi_p->page);
	}
}

int push_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p, const locked_page_info* lpi_p)
{
	locked_page_info* to_push = malloc(sizeof(locked_page_info));
	(*to_push) = (*lpi_p);
	return push_back_to_arraylist(btlps_p, to_push);
}

int pop_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p)
{
	locked_page_info* to_pop = (locked_page_info*)get_back_of_arraylist(btlps_p);
	int popped = pop_back_from_arraylist(btlps_p);
	if(popped)
		free(to_pop);
	return popped;
}

locked_page_info* get_top_stack_bplus_tree_locked_pages_stack(const arraylist* btlps_p)
{
	return (locked_page_info*)get_back_of_arraylist(btlps_p);
}

void fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(arraylist* btlps_p, const data_access_methods* dam_p)
{
	while(!is_empty_arraylist(btlps_p))
	{
		locked_page_info* to_unlock = (locked_page_info*)get_front_of_arraylist(btlps_p);
		pop_front_from_arraylist(btlps_p);
		unlock_page_and_delete_locked_page_info(to_unlock, 0, 0, dam_p);
		free(to_unlock);
	}
}