#ifndef PAGE_LIST_PAGE_UTIL_H
#define PAGE_LIST_PAGE_UTIL_H

#include<stdint.h>

#include<tuple_def.h>

#include<page_list_util.h>

// to initialize a page list page
int init_list_page(void* page, uint32_t page_size, const tuple_def* record_def);

uint32_t get_next_list_page(const void* page);
int set_next_list_page(void* page, uint32_t next_page_id);

#endif