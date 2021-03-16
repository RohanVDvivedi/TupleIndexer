#ifndef PAGE_LIST_DELETE_H
#define PAGE_LIST_DELETE_H

#include<page_list.h>

// returns 1, the page was merge was performed
// splits and merges are performed in the direction of traversal of the given page_cursor
int merge_page_at_cursor(page_cursor* pc_p);

// no seek is performed (the number of tuples already seeked remains the same)
// returns 0 => failure, 1 => succcess w/o page merge, 2 => success with page merge
int delete_tuple_at_cursor(page_cursor* pc_p);

#endif