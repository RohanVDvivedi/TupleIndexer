#ifndef LOCKED_PAGES_STACK_H
#define LOCKED_PAGES_STACK_H

#include<stdint.h>
#include<tupleindexer/utils/persistent_page.h>

#include<cutlery/value_arraylist.h>

typedef struct locked_page_info locked_page_info;
struct locked_page_info
{
	// page_id of the page and pointer to the page
	persistent_page ppage;

	// only used if level > 0
	// it signifies the direction that we took while traversing the bplus_tree through this interior node
	uint32_t child_index;
};

// macro to initialize locked_page_info struct on stack
#define INIT_LOCKED_PAGE_INFO(ppage_val, child_index_val) ((locked_page_info){.ppage = ppage_val, .child_index = child_index_val})

data_definitions_value_arraylist(locked_pages_stack, locked_page_info)

int initialize_locked_pages_stack(locked_pages_stack* lps, cy_uint capacity);

void deinitialize_locked_pages_stack(locked_pages_stack* lps);

// returns number of locked_page_info s inside the stack
cy_uint get_element_count_locked_pages_stack(const locked_pages_stack* lps);

// returns capacity of the locked_pages_stack to hold locked_page_info s
cy_uint get_capacity_locked_pages_stack(const locked_pages_stack* lps);

// pushes the locked_page_info to the locked_pages_stack
int push_to_locked_pages_stack(locked_pages_stack* lps, const locked_page_info* lpi_p);

// pops the locked_page_info of a locked page from the locked_pages_stack
int pop_from_locked_pages_stack(locked_pages_stack* lps);

// returns pointer to the top locked_page_info of the locked_pages_stack
locked_page_info* get_top_of_locked_pages_stack(const locked_pages_stack* lps);

// returns pointer to the bottom locked_page_info of the locked_pages_stack
locked_page_info* get_bottom_of_locked_pages_stack(const locked_pages_stack* lps);

// returns pointer to the nth element in locked_pages_stack (counting from bottom)
locked_page_info* get_from_bottom_of_locked_pages_stack(const locked_pages_stack* lps, cy_uint index);

int pop_bottom_from_locked_pages_stack(locked_pages_stack* lps);

#endif