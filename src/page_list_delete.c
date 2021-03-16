#include<page_list_delete.h>

#include<page_list_util.h>

int merge_page_at_cursor(page_cursor* pc_p);

int delete_tuple_at_cursor(page_cursor* pc_p)
{
	if(pc_p->lock_type != WRITER_LOCK)
		return 0;

	return 0;
}