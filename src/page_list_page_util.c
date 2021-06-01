#include<page_list_page_util.h>

#include<page_layout.h>

int is_list_page(const void* page)
{
	return get_page_type(page) == PAGE_LIST_PAGE_TYPE;
}

int init_list_page(void* page, uint32_t page_size, const tuple_def* record_def)
{
	return init_page(page, page_size, PAGE_LIST_PAGE_TYPE, 1, record_def);
}

uint32_t get_next_list_page(const void* page)
{
	return get_reference_page_id(page, NEXT_PAGE_ID);
}

int set_next_list_page(void* page, uint32_t next_page_id)
{
	return set_reference_page_id(page, NEXT_PAGE_ID, next_page_id);
}

uint16_t get_record_count_in_list_page(const void* page)
{
	return get_tuple_count(page);
}

const void* get_record_from_list_page(const void* page, uint32_t page_size, uint16_t index, const tuple_def* record_def)
{
	return get_nth_tuple(page, page_size, record_def, index);
}