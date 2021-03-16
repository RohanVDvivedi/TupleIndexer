#ifndef PAGE_LIST_SEEK_H
#define PAGE_LIST_SEEK_H

#include<page_list.h>

// all seek functions
// return pointer to the tuple
// returns NULL (if the end of page_list is reached)

const void* seek_cursor_to_next_tuple(page_cursor* pc_p);
const void* seek_cursor_to_next_page_first_tuple(page_cursor* pc_p);
const void* seek_cursor_to_current_page_first_tuple(page_cursor* pc_p);

#endif