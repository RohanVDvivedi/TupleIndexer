#include<page_list.h>

#include<stdlib.h>
#include<string.h>

int create_new_page_list(page_list_handle* plh, tuple_def* record_def, uint32_t head_id)
{
	initialize_rwlock(&(plh->handle_lock));
	plh->head_id = head_id;
	plh->record_def = malloc(size_of_tuple_def(record_def->element_count));
	memcpy(plh->record_def, record_def, size_of_tuple_def(record_def->element_count));
	return 1;
}

int deinit_page_list(page_list_handle* plh)
{
	deinitialize_rwlock(&(plh->handle_lock));
	plh->head_id = NULL_PAGE_REF;
	return 1;
}

int init_read_cursor(page_list_handle* plh, read_cursor* rc, const data_access_methods* dam_p)
{
	if(plh->head_id == NULL_PAGE_REF)
		return 0;
	return lock_page_and_open_read_cursor(rc, plh->head_id, 0, plh->record_def, dam_p);
}

int init_writable_handle(page_list_handle* plh, page_list_writable_handle* plwh, const data_access_methods* dam_p)
{
	if(plh->head_id == NULL_PAGE_REF)
		return 0;

	plwh->parent_page_list = plh;
	write_lock(&(plh->handle_lock));

	plwh->prev_page = NULL;
	plwh->is_curr_the_first_page = 1;
	plwh->curr_page = dam_p->acquire_page_with_writer_lock(dam_p->context, plh->head_id);

	return 1;
}