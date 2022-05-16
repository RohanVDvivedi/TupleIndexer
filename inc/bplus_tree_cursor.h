#ifndef BPLUS_TREE_CURSOR_H
#define BPLUS_TREE_CURSOR_H

#include<stdint.h>

#include<data_access_methods.h>
#include<bplus_tree_tuple_definitions.h>

typedef struct bplus_tree_cursor bplus_tree_cursor;
struct bplus_tree_cursor
{
	void* curr_page;

	uint64_t curr_page_id;

	uint32_t curr_tuple_index;

	const bplus_tree_tuple_defs* bpttd_p;

	const data_access_methods* dam_p;
};

bplus_tree_cursor* get_new_bplus_tree_cursor(void* curr_page, uint64_t curr_page_id, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

// it moves the cursor forward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
int next_bplus_tree_cursor(bplus_tree_cursor* cursor);

// returns pointer to the current tuple that the cursor points to
const void* get_curr_bplus_tree_cursor(bplus_tree_cursor* cursor);

// it moves the cursor backward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
int prev_bplus_tree_cursor(bplus_tree_cursor* cursor);

int destroy_bplus_tree_cursor(bplus_tree_cursor* cursor);

#endif