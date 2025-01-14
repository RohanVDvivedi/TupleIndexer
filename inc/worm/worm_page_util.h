#ifndef WORM_PAGE_UTIL_H
#define WORM_PAGE_UTIL_H

#include<stdint.h>

#include<persistent_page.h>
#include<worm_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// initializes a new worm head page, with tail_page_id and next_page_id = NULL_PAGE_ID
// reference_count (must be non zero), dependent_root
int init_worm_head_page(persistent_page* ppage, uint32_t reference_count, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// initializes a new any worm page, with next_page_id = NULL_PAGE_ID
int init_worm_any_page(persistent_page* ppage, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// if this function returns 0, there is no more any space on this page, try adding a new tail page
// else attempt to insert a blob with at most this size and it will succeed in being appended on the worm page
uint32_t blob_bytes_insertable_for_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

#endif