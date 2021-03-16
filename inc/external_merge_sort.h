#ifndef EXTERNAL_MERGE_SORT_H
#define EXTERNAL_MERGE_SORT_H

#include<page_list.h>

// external merge sort

// here : key_elements_count denote the number of elements to sort on
void external_merge_sort_the_page_list(uint32_t page_list_head_page_id, uint16_t key_elements_count, const data_access_methods* dam_p);

#endif