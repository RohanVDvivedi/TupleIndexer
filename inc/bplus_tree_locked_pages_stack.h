#ifndef BPLUS_TREE_LOCKED_PAGES_STACK_H
#define BPLUS_TREE_LOCKED_PAGES_STACK_H

#include<arraylist.h>

#include<data_access_methods.h>

typedef struct locked_page_info locked_page_info;
struct locked_page_info
{
	// page_id of the page
	uint64_t page_id;

	// pointer to the page
	void* page;

	// level of the locked page
	// 0 implies a leaf page
	uint32_t level;

	// 1 => we have a write lock on the page
	// 0 implies a read lock
	int is_write_locked;

	// 1 implies that the locked page is root of the bplus_tree
	int is_root;

	// only used if level > 0
	// it signifies the direction that we took while traversing the bplus_tree through this interior node
	uint32_t child_index;
};

// in case of an error to get read/write lock on the page, this function returns a NULL
locked_page_info* lock_page_and_get_new_locked_page_info(uint64_t page_id, int get_write_lock, int is_root, data_access_methods* dam_p);

int unlock_page_and_delete_locked_page_info(locked_page_info* lpi_p, data_access_methods* dam_p);

// pushes the locked page info of a locked page to the stack
void push_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p, locked_page_info* lpi_p);

// pops the locked page info of a locked page from the stack
locked_page_info* pop_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p);

// returns top of the locked page info of a locked page from the stack
locked_page_info* get_top_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p);

// unlocks all the pages in the same order as they were pushed (first in first out)
// unlocks pages in direction from root to leaf (and may not include root and leaf pages themselves, only the pages whose info have been pushed)
// it also frees up memory that is being used by all the lock_page_info structs
void fifo_unlock_all_bplus_tree_locked_pages_stack(arraylist* btlps_p, data_access_methods* dam_p);

#endif