#ifndef PAGE_LIST_INSERT_H
#define PAGE_LIST_INSERT_H

#include<page_list.h>

// returns 1, the page was split was performed
// splits and merges are performed in the direction of traversal of the given page_cursor
int split_page_at_cursor(page_cursor* pc_p);

// the tuple is inserted after the cursor if the flag "after_cursor" is set
// no seek is performed (the number of tuples already seeked remains the same)
// returns 0 => failure, 1 => succcess w/o page split, 2 => success with page split
int insert_tuple_at_cursor(page_cursor* pc_p, int after_cursor, const void* tuple_to_insert);

#endif