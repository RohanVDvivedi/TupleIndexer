#include<bplus_tree_locked_pages_stack.h>

#include<stdlib.h>

#include<bplus_tree_page.h>

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
		if(to_unlock->is_write_locked)
			dam_p->release_writer_lock_on_page(dam_p->context, to_unlock->page, 0);
		else
			dam_p->release_reader_lock_on_page(dam_p->context, to_unlock->page);
		free(to_unlock);
	}
}