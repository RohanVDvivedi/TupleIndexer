#include<read_cursor.h>

#include<page_list_util.h>

int lock_page_and_open_read_cursor(read_cursor* rc, uint32_t read_page_id, uint32_t record_id, const tuple_def* record_def, const data_access_methods* dam_p)
{
	rc->read_page = dam_p->acquire_page_with_reader_lock(dam_p->context, read_page_id);
	if(rc->read_page == NULL)
		return 0;
	rc->record_def = record_def;
	rc->record_id = record_id;
	return 1;
}

int open_read_cursor(read_cursor* rc, const void* read_page, uint32_t record_id, const tuple_def* record_def)
{
	rc->read_page = read_page;
	rc->record_def = record_def;
	rc->record_id = record_id;
	return 1;
}

int seek_next_read_cursor(read_cursor* rc, const data_access_methods* dam_p)
{
	if(get_record_count_in_leaf_page(rc->read_page) > 0 && 
		rc->record_id < get_record_count_in_leaf_page(rc->read_page) - 1)
	{
		rc->record_id++;
		if(rc->record_id < get_record_count_in_leaf_page(rc->read_page))
			return 1;
	}

	do
	{
		uint32_t next_sibling_page_id = get_next_sibling_leaf_page(rc->read_page);
		if(next_sibling_page_id != NULL_PAGE_REF)
		{
			void* next_sibling_page = dam_p->acquire_page_with_reader_lock(dam_p->context, next_sibling_page_id);
			dam_p->release_reader_lock_on_page(dam_p->context, rc->read_page);
			rc->read_page = next_sibling_page;
			rc->record_id = 0;
		}
		else
			return 0;	// i.e. end of leaf pages
	}
	while(get_record_count_in_leaf_page(rc->read_page) == 0);
	// if pointing to empty page then re iterate

	return 1;
}

const void* get_record_from_read_cursor(read_cursor* rc, const data_access_methods* dam_p)
{
	return get_nth_tuple(rc->read_page, dam_p->page_size, rc->record_def, rc->record_id);
}

void close_read_cursor(read_cursor* rc, const data_access_methods* dam_p)
{
	dam_p->release_reader_lock_on_page(dam_p->context, rc->read_page);
	rc->read_page = NULL;
	rc->record_id = 0;
}