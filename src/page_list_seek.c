#include<page_list_seek.h>

#include<page_list_util.h>

const void* seek_cursor_to_current_page_first_tuple(page_cursor* pc_p)
{
	pc_p->tuple_id = 0;
	return (get_tuple_count(pc_p->page) == 0) ? NULL : seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_id);
}

const void* seek_cursor_to_next_page_first_tuple(page_cursor* pc_p)
{
	uint32_t next_page_id = get_reference_page_id(pc_p->page, NEXT_PAGE_REFERENCE_INDEX);
	void* old_page = pc_p->page;

	// can not jump to a NULL page
	if(next_page_id == NULL_PAGE_REFERENCE)
		return NULL;

	pc_p->page = acquire_lock(pc_p, next_page_id);
	release_lock(pc_p, old_page);

	pc_p->page_id = next_page_id;
	pc_p->tuple_id = 0;

	return (get_tuple_count(pc_p->page) == 0) ? NULL : seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_id);
}

const void* seek_cursor_to_next_tuple(page_cursor* pc_p)
{
	pc_p->tuple_id++;
	return (pc_p->tuple_id == get_tuple_count(pc_p->page)) ? seek_cursor_to_next_page_first_tuple(pc_p) : seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_id);
}