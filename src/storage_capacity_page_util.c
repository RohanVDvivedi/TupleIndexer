#include<storage_capacity_page_util.h>

#include<page_layout.h>

int is_page_lesser_than_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t free_space = get_free_space_in_page(page, page_size, def);
	uint32_t used_space = allotted_space - free_space;
	return used_space < (allotted_space / 2);
}

int is_page_lesser_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t free_space = get_free_space_in_page(page, page_size, def);
	uint32_t used_space = allotted_space - free_space;
	return used_space <= (allotted_space / 2);
}

int is_page_more_than_or_equal_to_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t free_space = get_free_space_in_page(page, page_size, def);
	uint32_t used_space = allotted_space - free_space;
	return used_space >= (allotted_space / 2);
}

int is_page_more_than_half_full(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t free_space = get_free_space_in_page(page, page_size, def);
	uint32_t used_space = allotted_space - free_space;
	return used_space > (allotted_space / 2);
}

float get_storage_capacity_used_for_page(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t free_space     = get_free_space_in_page(page, page_size, def);
	uint32_t used_space = allotted_space - free_space;
	return (((float)used_space) / allotted_space);
}

float get_storage_efficiency_for_page(const void* page, uint32_t page_size, const tuple_def* def)
{
	uint32_t allotted_space = get_space_allotted_to_all_tuples(page, page_size, def);
	uint32_t free_space     = get_free_space_in_page(page, page_size, def);

	uint32_t occupied_space = get_space_occupied_by_all_tuples(page, page_size, def);
	uint32_t used_space = allotted_space - free_space;

	if(used_space == 0)
		return 1.0;

	return (((float)occupied_space) / used_space);
}