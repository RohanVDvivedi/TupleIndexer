#include<locked_pages_stack.h>

declarations_value_arraylist(locked_pages_stack, locked_page_info)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(locked_pages_stack, locked_page_info)

int push_to_locked_pages_stack(locked_pages_stack* lps, const locked_page_info* lpi_p)
{
	return push_back_to_locked_pages_stack(lps, lpi_p);
}

int pop_from_locked_pages_stack(locked_pages_stack* lps)
{
	return pop_back_from_locked_pages_stack(lps);
}

locked_page_info* get_top_of_locked_pages_stack(const locked_pages_stack* lps)
{
	return (locked_page_info*)(get_back_of_locked_pages_stack(lps));
}

locked_page_info* get_bottom_of_locked_pages_stack(const locked_pages_stack* lps)
{
	return (locked_page_info*)(get_front_of_locked_pages_stack(lps));
}

int pop_bottom_from_locked_pages_stack(locked_pages_stack* lps)
{
	return pop_front_from_locked_pages_stack(lps);
}