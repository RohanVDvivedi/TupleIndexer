#ifndef BPLUS_TREE_LOCKED_PAGES_STACK_H
#define BPLUS_TREE_LOCKED_PAGES_STACK_H

#include<arraylist.h>

#include<bplus_tree_tuple_definitions.h>
#include<data_access_methods.h>

typedef struct locked_page_info locked_page_info;
struct locked_page_info
{
	// page_id of the page
	uint64_t page_id;

	// pointer to the page
	void* page;

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
// the page is locked with a write lock if get_write_lock == 1, and with a read lock if it is 0
locked_page_info* lock_page_and_get_new_locked_page_info(uint64_t page_id, int get_write_lock, int is_root, const data_access_methods* dam_p);

// returns a locked_page_info for an already locked page
locked_page_info* get_new_locked_page_info(void* page, uint64_t page_id, int write_locked, int is_root);

// you can also request to make the page free, by passing should_free_this_page = 1
int unlock_page_and_delete_locked_page_info(locked_page_info* lpi_p, int should_free_this_page, int was_modified_if_write_lock, const data_access_methods* dam_p);

// pushes the locked page info of a locked page to the stack
int push_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p, locked_page_info* lpi_p);

// pops the locked page info of a locked page from the stack
locked_page_info* pop_stack_bplus_tree_locked_pages_stack(arraylist* btlps_p);

// returns top of the locked page info of a locked page from the stack
locked_page_info* get_top_stack_bplus_tree_locked_pages_stack(const arraylist* btlps_p);

// unlocks all the pages in the same order as they were pushed (first in first out)
// unlocks pages in direction from root to leaf (and may not include root and leaf pages themselves, only the pages whose info have been pushed)
// it also frees up memory that is being used by all the lock_page_info structs
// it only unlocks the pages, it does not free those pages, THIS IS EXPECTED BEHAVIOUR
// all the pages were marked as was_modified == 0, if the lock was a write lock
void fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(arraylist* btlps_p, const data_access_methods* dam_p);

#endif