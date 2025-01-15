#ifndef WORM_H
#define WORM_H

/* WORM = Write-Once-Read-Multiple-times data structure
** use it for storing larger than page data types, or types that do not fit on page
** also has a reference counter and the data in worm and that of dependent root page id are destroyed when the reference count reaches 0
** each page has atleast 1 partial_blob_type's tuple_def on it
*/

#include<worm_tuple_definitions_public.h>

#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// if reference_counter == 0, then it is forced to 1
uint64_t get_new_worm(uint64_t reference_counter, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int increment_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// performs a decrement on the worm's reference_counter, and if it reaches 0, then the worm is destroyed
int decrement_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_worm(uint64_t root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#endif