#ifndef PAGE_LIST_PAGE_UTIL_H
#define PAGE_LIST_PAGE_UTIL_H

#include<stdint.h>

#include<tuple_def.h>

#include<page_list_util.h>

// to initialize a page list page
int init_list_page(void* page, uint32_t page_size, const tuple_def* record_def);

uint32_t get_next_list_page(const void* page);
int set_next_list_page(void* page, uint32_t next_page_id);

// returns number of records in the list page
uint16_t get_record_count_in_list_page(const void* page);

// returns a pointer in the page that points to index-th record, and has tuple definition of that of a record_def
const void* get_record_from_list_page(const void* page, uint32_t page_size, uint16_t index, const tuple_def* record_def);

#endif