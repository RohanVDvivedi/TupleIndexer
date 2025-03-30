#ifndef ARRAY_TABLE_PAGE_UTIL_H
#define ARRAY_TABLE_PAGE_UTIL_H

#include<tupleindexer/array_table/array_table_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/utils/bucket_range.h>

// initialize array table page
int init_array_table_page(persistent_page* ppage, uint32_t level, uint64_t first_bucket_id, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// the first_bucket_id of the page is always a multiple of get_leaf_entries_refrenceable(level + 1)
uint64_t get_first_bucket_id_for_level_containing_bucket_id_for_array_table_page(uint32_t level, uint64_t bucket_id, const array_table_tuple_defs* attd_p);

// returns attd_p->leaf_entries_per_page for leaf pages (level = 0), else it returns attd_p->index_entries_per_page
uint32_t get_entries_per_page_for_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

// prints array table page
void print_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

// NOTE:: the below 7 functions allow you to access a array_table_page as an array of page_ids of size = attd_p->entries_per_page

// returns 1, if the child_index is not within the entries_per_page for the respective page types OR if the entry is really NULL
int is_NULL_at_child_index_in_array_table_page(const persistent_page* ppage, uint32_t child_index, const array_table_tuple_defs* attd_p);

// -- below 2 functions are for only the leaf pages of array_table

// returns the record in the leaf page at child_index
// return value is NULL if the child_entry is NULL, else it returns the record copied into the preallocated_memory
const void* get_record_entry_at_child_index_in_array_table_leaf_page(const persistent_page* ppage, uint32_t child_index, void* preallocated_memory, const array_table_tuple_defs* attd_p);

// set record in the page at child_index
// fails if child_index > attd_p->entries_per_page
int set_record_entry_at_child_index_in_array_table_leaf_page(persistent_page* ppage, uint32_t child_index, const void* record, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// -- below 2 functions are for only the index pages of array_table

// get child_page_id in the page at child_index from index_page
uint64_t get_child_page_id_at_child_index_in_array_table_index_page(const persistent_page* ppage, uint32_t child_index, const array_table_tuple_defs* attd_p);

// set child_page_id in the page at child_index
// fails if child_index > attd_p->entries_per_page
int set_child_page_id_at_child_index_in_array_table_index_page(persistent_page* ppage, uint32_t child_index, uint64_t child_page_id, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns true if all the child pointers in the array_table_page are NULL_PAGE_ID
// i.e. tombstones_count == tuples_count
int has_all_NULL_entries_in_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

// returns the number of children of the page that are not to NULL
// you may level down this index page, only if this value is 1, yet you may also call this function on leaf pages
// in case this value becomes 0 (i.e. has_all_NULL_PAGE_ID_in_array_table_page(ppage, attd_p) == 1), then you may free this page and remove it's entry from the parent page
uint32_t get_non_NULL_entry_count_in_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

// NOTE:: the above 7 functions allow you to access a array_table_page as an array of page_ids of size = attd_p->entries_per_page

// returns bucket ranges that this page contains
// it returns [first_bucket_id, first_bucket_id + get_leaf_entries_refrenceable(level + 1) - 1] (both inclusive range)
bucket_range get_bucket_range_for_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

// returns the bucket range that this page entrusts its child at child_index to contain
// the delegated range of the root page is [0, UINT64_MAX]
// this function does not care about the overflow of the first_bucket_id of the delegated range, it is expected that you will always provide a child_index that has atleast 1 bucket
bucket_range get_delegated_bucket_range_for_child_index_on_array_table_page(const persistent_page* ppage, uint32_t child_index, const array_table_tuple_defs* attd_p);

// find the child_index to go to for accessing bucket at bucket_id index
// returns NO_TUPLE_FOUND, if the bucket_id is not in the range of this page
uint32_t get_child_index_for_bucket_id_on_array_table_page(const persistent_page* ppage, uint64_t bucket_id, const array_table_tuple_defs* attd_p);

// level up the arra table page, moving its contents into one of its children
// you must have write lock on the array_table_page to do this
// all the page locks acquired in this function will be released on its return
// NOTE: if the page is all NULL_PAGE_ID, then you do not need to level it up, instead re initialize as a leaf and valid first_bucket_id and insert into it
int level_up_array_table_page(persistent_page* ppage, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// level down array table page, moving contents of its children into it
// this succeeds only if there is exactly 1 non NULL_PAGE_ID child on the page, and the page is not a leaf
// you must have write lock on the array_table_page to do this
// all the page locks acquired in this function will be released on its return
int level_down_array_table_page(persistent_page* ppage, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif