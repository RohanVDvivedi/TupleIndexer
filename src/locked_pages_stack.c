#include<locked_pages_stack.h>

#include<stdlib.h>

#include<bplus_tree_page.h>

locked_pages_stack* new_locked_pages_stack(uint32_t capacity)
{
	locked_pages_stack* lps = malloc(sizeof_locked_pages_stack(capacity));
	lps->capacity = capacity;
	lps->start_index = 0;
	lps->element_count = 0;
	return lps;
}

void delete_locked_pages_stack(locked_pages_stack* lps)
{
	free(lps);
}

uint32_t get_element_count_locked_pages_stack(const locked_pages_stack* lps)
{
	return lps->element_count;
}

static inline uint32_t sum_circular(uint32_t x, uint32_t y, uint32_t max)
{
	if(x >= max - y)
		return x - (max - y);
	else
		return x + y;
}

static inline uint32_t sub_circular(uint32_t x, uint32_t y, uint32_t max)
{
	if(x < y)
		return max - (y - x);
	else
		return x - y;
}

int push_to_locked_pages_stack(locked_pages_stack* lps, const locked_page_info* lpi_p)
{
	if(lps->element_count == lps->capacity)
		return 0;
	if(lps->element_count == 0)
		lps->start_index = 0;
	uint32_t new_top_index = sum_circular(lps->start_index, lps->element_count, lps->capacity);
	lps->element_count++;
	lps->locked_page_infos[new_top_index] = *lpi_p;
	return 1;
}

int pop_from_locked_pages_stack(locked_pages_stack* lps)
{
	if(lps->element_count == 0)
		return 0;
	lps->element_count--;
	return 1;
}

locked_page_info* get_top_of_locked_pages_stack(const locked_pages_stack* lps)
{
	if(lps->element_count == 0)
		return NULL;
	return (locked_page_info*)(lps->locked_page_infos + sum_circular(lps->start_index, lps->element_count - 1, lps->capacity));
}

locked_page_info* get_bottom_of_locked_pages_stack(const locked_pages_stack* lps)
{
	if(lps->element_count == 0)
		return NULL;
	return (locked_page_info*)(lps->locked_page_infos + lps->start_index);
}

int pop_bottom_from_locked_pages_stack(locked_pages_stack* lps)
{
	if(lps->element_count == 0)
		return 0;
	lps->element_count--;
	lps->start_index = sum_circular(lps->start_index, 1, lps->capacity);
	return 1;
}