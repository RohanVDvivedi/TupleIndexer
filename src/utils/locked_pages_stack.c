#include<locked_pages_stack.h>

data_definitions_value_arraylist(locked_pages_stack_INTERNAL, locked_page_info)
declarations_value_arraylist(locked_pages_stack_INTERNAL, locked_page_info, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(locked_pages_stack_INTERNAL, locked_page_info, static inline)

int initialize_locked_pages_stack(locked_pages_stack* lps, cy_uint capacity)
{
	return initialize_locked_pages_stack_INTERNAL((locked_pages_stack_INTERNAL*)lps, capacity);
}

void deinitialize_locked_pages_stack(locked_pages_stack* lps)
{
	deinitialize_locked_pages_stack_INTERNAL((locked_pages_stack_INTERNAL*)lps);
}

cy_uint get_element_count_locked_pages_stack(const locked_pages_stack* lps)
{
	return get_element_count_locked_pages_stack_INTERNAL((const locked_pages_stack_INTERNAL*)lps);
}

cy_uint get_capacity_locked_pages_stack(const locked_pages_stack* lps)
{
	return get_capacity_locked_pages_stack_INTERNAL((const locked_pages_stack_INTERNAL*)lps);
}

int push_to_locked_pages_stack(locked_pages_stack* lps, const locked_page_info* lpi_p)
{
	return push_back_to_locked_pages_stack_INTERNAL((locked_pages_stack_INTERNAL*)lps, lpi_p);
}

int pop_from_locked_pages_stack(locked_pages_stack* lps)
{
	return pop_back_from_locked_pages_stack_INTERNAL((locked_pages_stack_INTERNAL*)lps);
}

locked_page_info* get_top_of_locked_pages_stack(const locked_pages_stack* lps)
{
	return (locked_page_info*)(get_back_of_locked_pages_stack_INTERNAL((const locked_pages_stack_INTERNAL*)lps));
}

locked_page_info* get_bottom_of_locked_pages_stack(const locked_pages_stack* lps)
{
	return (locked_page_info*)(get_front_of_locked_pages_stack_INTERNAL((const locked_pages_stack_INTERNAL*)lps));
}

locked_page_info* get_from_bottom_of_locked_pages_stack(const locked_pages_stack* lps, cy_uint index)
{
	return (locked_page_info*)(get_from_front_of_locked_pages_stack_INTERNAL((const locked_pages_stack_INTERNAL*)lps, index));
}

int pop_bottom_from_locked_pages_stack(locked_pages_stack* lps)
{
	return pop_front_from_locked_pages_stack_INTERNAL((locked_pages_stack_INTERNAL*)lps);
}