#ifndef PAGE_TABLE_RANGE_LOCKER_PUBLIC_H
#define PAGE_TABLE_RANGE_LOCKER_PUBLIC_H

#include<page_table_bucket_range.h>

typedef struct page_table_range_locker page_table_range_locker;

// creates a new page_table_range_locker for the given range
page_table_range_locker* get_new_page_table_range_locker(uint64_t root_page_id, page_table_bucket_range lock_range, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p);

// minimizes the lock range of the range_locker
int minimize_lock_range_for_page_table_range_locker(page_table_range_locker* ptrl_p, page_table_bucket_range lock_range);

// get lock range for the page_table_range_locker
page_table_bucket_range get_lock_range_for_page_table_range_locker(const page_table_range_locker* ptrl_p);

// check if the page_table_range_locker is locked for writing
// you may set only if this returns 1
int is_writable_page_table_range_locker(const page_table_range_locker* ptrl_p);

// you may only get, if the bucket_id is within get_lock_range_for_page_table_range_locker()
uint64_t get_from_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id);

// you may only set, if the bucket_id is within get_lock_range_for_page_table_range_locker() and if the ptrl is writable, returns 0 other wise
int set_in_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, uint64_t page_id);

// destroys the page_table_range_locker
void destroy_page_table_range_locker(page_table_range_locker* ptrl_p);

#endif