#include<page_list_writable_handle.h>

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

int close_page_list_writable_handle(page_list_writable_handle* plwh, const data_access_methods* dam_p);