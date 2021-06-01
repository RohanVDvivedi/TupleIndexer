#include<page_list_page_util.h>

#include<page_layout.h>

int init_list_page(void* page, uint32_t page_size, const tuple_def* record_def)
{
	int inited = init_page(page, page_size, PAGE_LIST_PAGE_TYPE, 1, record_def);
	if(inited)
		set_reference_page_id(page, NEXT_PAGE_REF, NULL_PAGE_REF);
	return inited;
}