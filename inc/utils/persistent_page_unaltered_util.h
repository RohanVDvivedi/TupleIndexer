#ifndef PERSISTENT_PAGE_UNALTERED_UTIL_H
#define PERSISTENT_PAGE_UNALTERED_UTIL_H

#include<persistent_page_functions.h>

// resiliently here implies forcefully
// below functions will ensure that if the operation could succeed after a compaction, then they will return true

int can_append_tuple_on_persistent_page_if_done_resiliently(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple);

int can_insert_tuple_on_persistent_page_if_done_resiliently(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

int can_update_tuple_on_persistent_page_if_done_resiliently(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

#endif