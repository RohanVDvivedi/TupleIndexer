#include<storage_capacity_page_util.h>

#include<page_layout.h>

int is_page_lesser_than_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t used_space = get_space_occupied_by_all_tuples(page, page_size, def);
	return used_space < (allotted_space / 2);
}

int is_page_lesser_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t used_space = get_space_occupied_by_all_tuples(page, page_size, def);
	return used_space <= (allotted_space / 2);
}

int is_page_more_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t used_space = get_space_occupied_by_all_tuples(page, page_size, def);
	return used_space >= (allotted_space / 2);
}

int is_page_more_than_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t used_space = get_space_occupied_by_all_tuples(page, page_size, def);
	return used_space > (allotted_space / 2);
}