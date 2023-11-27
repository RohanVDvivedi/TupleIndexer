#ifndef PAGE_TABLE_PAGE_UTIL_H
#define PAGE_TABLE_PAGE_UTIL_H

#include<page_table_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page.h>
#include<page_table_bucket_range.h>

// initialize page table page
int init_page_table_page(persistent_page* ppage, uint32_t level, uint64_t first_bucket_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// the first_bucket_id of the page is always a multipl of (entries_per_page ^ (level + 1))
uint64_t get_first_bucket_id_for_level_containing_bucket_id_for_page_table_page(const persistent_page* ppage, uint32_t level, uint64_t bucket_id, const page_table_tuple_defs* pttd_p);

// prints page table page
void print_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

// NOTE:: the below 4 functions allow you to access a page_table_page as an array of page_ids of size = pttd_p->entries_per_page

// get child_page_id in the page at child_index
uint64_t get_child_page_id_at_child_index_in_page_table_page(const persistent_page* ppage, uint32_t child_index, const page_table_tuple_defs* pttd_p);

// set child_page_id in the page at child_index
// fails if child_index > pttd_p->entries_per_page
int set_child_page_id_at_child_index_in_page_table_page(persistent_page* ppage, uint32_t child_index, uint64_t child_page_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns true if all the child pointers in the page_table_page are NULL_PAGE_ID
// i.e. tombstones_count == tuples_count
int has_all_NULL_PAGE_ID_in_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

// returns the number of children of the page that are not equal to NULL_PAGE_ID
// you may shrink up the level, only if this valus is 1
// in case this value becomes 0 (i.e. has_all_NULL_PAGE_ID_in_page_table_page(ppage, pttd_p) == 1), then you may free this page and remove it's entry from the parent page
uint32_t get_non_NULL_PAGE_ID_count_in_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

// NOTE:: the above 4 functions allow you to access a page_table_page as an array of page_ids of size = pttd_p->entries_per_page

// returns bucket ranges that this page contains
// it returns [first_bucket_id, first_bucket_id + entries_per_page ^ (level + 1) - 1] (both inclusive range)
page_table_bucket_range get_bucket_range_for_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

// returns the bucket range that this page entrusts its child at child_index to contain
// the delegated range of the root page is [0, UINT64_MAX]
// this function does not care about the overflow of the first_bucket_id of the delegated range, it is expected that you will always provide a child_index that has atleast 1 bucket
page_table_bucket_range get_delegated_bucket_range_for_child_index_on_page_table_page(const persistent_page* ppage, uint32_t child_index, const page_table_tuple_defs* pttd_p);

// find the child_index to go to for accessing bucket at bucket_id index
// returns NO_TUPLE_FOUND, if the bucket_id is not in the range of this page
uint32_t get_child_index_for_bucket_id_on_page_table_page(const persistent_page* ppage, uint64_t bucket_id, const page_table_tuple_defs* pttd_p);

// level up the page table page, moving its contents into one of its children
// you must have write lock on the page_table_page to do this
// all the page locks acquired in this function will be released on its return
int level_up_page_table_page(persistent_page* ppage, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// level down page table page, moving contents of its children into it
// this succeeds only if there is exactly 1 non NULL_PAGE_ID child on the page, and the page is not a leaf
// you must have write lock on the page_table_page to do this
// all the page locks acquired in this function will be released on its return
int level_down_page_table_page(persistent_page* ppage, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif