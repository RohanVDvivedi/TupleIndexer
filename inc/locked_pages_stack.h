#ifndef LOCKED_PAGES_STACK_H
#define LOCKED_PAGES_STACK_H

typedef struct locked_page_info locked_page_info;
struct locked_page_info
{
	// page_id of the page
	uint64_t page_id;

	// pointer to the page
	void* page;

	// only used if level > 0
	// it signifies the direction that we took while traversing the bplus_tree through this interior node
	uint32_t child_index;
};

// macro to initialize locked_page_info struct on stack
#define INIT_LOCKED_PAGE_INFO(page_val, page_id_val) ((locked_page_info){.page_id = page_id_val, .page = page_val, .child_index = -1})

typedef struct locked_pages_stack locked_pages_stack;
struct locked_pages_stack
{
	uint32_t capacity;
	uint32_t start_index;
	uint32_t element_count;
	locked_page_info locked_page_infos[];
};

#define sizeof_locked_pages_stack(capacity) (sizeof(locked_pages_stack) + capacity * sizeof(locked_page_info))

locked_pages_stack* new_locked_pages_stack(uint32_t capacity);

void delete_locked_pages_stack(locked_pages_stack* lps);

// returns number of locked_page_info s inside the stack
uint32_t get_element_count_locked_pages_stack(const locked_pages_stack* lps);

// pushes the locked_page_info to the locked_pages_stack
int push_to_locked_pages_stack(locked_pages_stack* lps, const locked_page_info* lpi_p);

// pops the locked_page_info of a locked page from the locked_pages_stack
int pop_from_locked_pages_stack(locked_pages_stack* lps);

// returns pointer to the top locked_page_info of the locked_pages_stack
locked_page_info* get_top_of_locked_pages_stack(const locked_pages_stack* lps);

// returns pointer to the bottom locked_page_info of the locked_pages_stack
locked_page_info* get_bottom_of_locked_pages_stack(const locked_pages_stack* lps);

int pop_bottom_from_locked_pages_stack(locked_pages_stack* lps);

#endif