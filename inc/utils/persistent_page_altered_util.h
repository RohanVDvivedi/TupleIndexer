#ifndef PERSISTENT_PAGE_UNALTERED_UTIL_H
#define PERSISTENT_PAGE_UNALTERED_UTIL_H

#include<persistent_page_functions.h>

int append_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error);

int insert_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error);

int update_tuple_on_persistent_page_resiliently(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error);

#endif