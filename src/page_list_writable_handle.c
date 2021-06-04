#include<page_list_writable_handle.h>

#include<page_list.h>

void* get_curr_page(page_list_writable_handle* plwh)
{
	return plwh->curr_page;
}

void* get_prev_page(page_list_writable_handle* plwh)
{
	return plwh->prev_page;
}

int seek_to_next_page(page_list_writable_handle* plwh, const data_access_methods* dam_p);

int insert_new_page_after_curr_page(page_list_writable_handle* plwh, const data_access_methods* dam_p);

int insert_new_page_before_curr_page(page_list_writable_handle* plwh, const data_access_methods* dam_p);

int delete_curr_page(page_list_writable_handle* plwh, const data_access_methods* dam_p);

int close_page_list_writable_handle(page_list_writable_handle* plwh, const data_access_methods* dam_p)
{
	if(plwh->is_curr_the_first_page)
	{
		write_unlock(&(plwh->parent_page_list->handle_lock));
		plwh->parent_page_list = NULL;
	}
	else
	{
		dam_p->release_writer_lock_on_page(dam_p->context, plwh->prev_page);
		plwh->prev_page = NULL;
	}

	plwh->is_curr_the_first_page = 0;

	dam_p->release_writer_lock_on_page(dam_p->context, plwh->curr_page);
	plwh->curr_page = NULL;

	return 1;
}