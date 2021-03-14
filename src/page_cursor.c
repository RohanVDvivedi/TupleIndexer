#include<page_cursor.h>

int initialize_cursor(page_cursor* pc_p, page_list* pl_p, const data_access_methods* dam_p)
{
	pc_p->pagelist = pl_p;
	pc_p->page_id = NULL_PAGE_REFERENCE;
	pc_p->page = NULL;
	pc_p->tuple_id = 0;
	pc_p->tuple = NULL;
}

int delete_tuple_at_the_cursor(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int seek_to_next_tuple_in_page_list(page_cursor* pc_p, const data_access_methods* dam_p)
{
	
}

int deinitialize_cursor(page_cursor* pc_p)
{

}