#include<page_list_page_util.h>

#include<page_layout.h>

int init_list_page(void* page, uint32_t page_size, const tuple_def* record_def)
{
	int inited = init_page(page, page_size, PAGE_LIST_PAGE_TYPE, 1, record_def);
	if(inited)
		set_reference_page_id(page, NEXT_PAGE_ID, NULL_PAGE_REF);
	return inited;
}

uint32_t get_next_list_page(const void* page)
{
	return get_reference_page_id(page, NEXT_PAGE_ID);
}

int set_next_list_page(void* page, uint32_t next_page_id)
{
	return set_reference_page_id(page, NEXT_PAGE_ID, next_page_id);
}