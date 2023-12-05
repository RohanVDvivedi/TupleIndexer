#ifndef PAGE_TABLE_RANGE_LOCKER_PUBLIC_H
#define PAGE_TABLE_RANGE_LOCKER_PUBLIC_H

#include<page_table_bucket_range.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page.h>
#include<find_position.h>

typedef struct page_table_range_locker page_table_range_locker;

// creates a new page_table_range_locker for the given range
// you want to call set then the lock_type must be WRITE_LOCK
// on abort_error, NULL is returned
page_table_range_locker* get_new_page_table_range_locker(uint64_t root_page_id, page_table_bucket_range lock_range, int lock_type, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// minimizes the lock range of the range_locker
// on an abort error, lock on the local root is released, then you only need to call delete_page_table_range_locker
int minimize_lock_range_for_page_table_range_locker(page_table_range_locker* ptrl_p, page_table_bucket_range lock_range, const void* transaction_id, int* abort_error);

// get lock range for the page_table_range_locker
page_table_bucket_range get_lock_range_for_page_table_range_locker(const page_table_range_locker* ptrl_p);

// check if the page_table_range_locker is locked for writing
// you may set only if this returns 1
int is_writable_page_table_range_locker(const page_table_range_locker* ptrl_p);

// you may only get, if the bucket_id is within get_lock_range_for_page_table_range_locker()
// on an abort error, lock on the local root is released, then you only need to call delete_page_table_range_locker
uint64_t get_from_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, const void* transaction_id, int* abort_error);

// you may only set, if the bucket_id is within get_lock_range_for_page_table_range_locker() and if the ptrl is writable, returns 0 other wise
// on an abort error, lock on the local root is released, then you only need to call delete_page_table_range_locker
int set_in_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, uint64_t page_id, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// finds bucket_id in page_table that is find_pos compared to the given bucket_id
// it will return the bucket_id (being an in-out parameter) and page_id
// if page_id == NULL_PAGE_ID, then no such bucket_id found
uint64_t find_non_NULL_PAGE_ID_in_page_table(page_table_range_locker* ptrl_p, uint64_t* bucket_id, find_position find_pos);

// deletes the page_table_range_locker, and releases lock on the local_root (if it is not NULL_persistent_page)
// we may need to unlock the local_root and descend down from the actual root, if the local_root becomes empty
// this is why we store the root_page_id in the ptrl
void delete_page_table_range_locker(page_table_range_locker* ptrl_p, const void* transaction_id, int* abort_error);

#endif