#ifndef BPLUS_TREE_CURSOR_H
#define BPLUS_TREE_CURSOR_H

#include<stdint.h>

typedef enum bplus_tree_cursor_direction bplus_tree_cursor_direction;
enum bplus_tree_cursor_direction
{
	TOWARDS_NEXT,
	TOWARDS_PREV,
};

typedef struct bplus_tree_cursor bplus_tree_cursor;
struct bplus_tree_cursor
{
	uint64_t curr_page_id;

	uint32_t curr_tuple_index;

	int curr_tuple_seen;	// 1 if the tuple pointed to by curr_page_id and curr_tuple_index has been returned to the user

	bplus_tree_cursor_direction direction;
};

#endif