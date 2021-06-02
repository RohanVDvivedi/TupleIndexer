#include<page_list.h>

int create_new_page_list(page_list_handle* plh)
{
	initialize_rwlock(&(plh->handle_lock));
	plh->head_id = NULL_PAGE_REF;
	return 1;
}

int init_read_cursor(page_list_handle* plh, read_cursor* rc, data_access_methods* dam_p)
{
	if(plh->head_id == NULL_PAGE_REF)
		return 0;
	// open read cursor
	return 1;
}

int init_writable_handle(page_list_handle* plh, page_list_writable_handle* plwh, data_access_methods* dam_p)
{
	if(plh->head_id == NULL_PAGE_REF)
		return 0;
	// open writable handle
	return 1;
}