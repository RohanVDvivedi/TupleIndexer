#ifndef PAGE_TABLE_RANGE_LOCKER_PUBLIC_H
#define PAGE_TABLE_RANGE_LOCKER_PUBLIC_H

#include<bucket_range.h>
#include<persistent_page.h>
#include<find_position.h>

typedef struct page_table_range_locker page_table_range_locker;

// creates a new page_table_range_locker for the given range
// you want to call set then the pmm_p must not be NULL
// on abort_error, NULL is returned
page_table_range_locker* get_new_page_table_range_locker(uint64_t root_page_id, bucket_range lock_range, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns NULL if ptrl_p is writable OR on an abort error
// on an abort_error, ptrl_p will still hold its locks
page_table_range_locker* clone_page_table_range_locker(const page_table_range_locker* ptrl_p, const void* transaction_id, int* abort_error);

// minimizes the lock range of the range_locker
// on an abort error, lock on the local root is released, then you only need to call delete_page_table_range_locker
int minimize_lock_range_for_page_table_range_locker(page_table_range_locker* ptrl_p, bucket_range lock_range, const void* transaction_id, int* abort_error);

// get lock range for the page_table_range_locker
bucket_range get_lock_range_for_page_table_range_locker(const page_table_range_locker* ptrl_p);

// check if the page_table_range_locker is locked for writing
// you may set only if this returns 1
int is_writable_page_table_range_locker(const page_table_range_locker* ptrl_p);

// you may only get, if the bucket_id is within get_lock_range_for_page_table_range_locker()
// on an abort error, lock on the local root is released, then you only need to call delete_page_table_range_locker
uint64_t get_from_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, const void* transaction_id, int* abort_error);

// you may only set, if the bucket_id is within get_lock_range_for_page_table_range_locker() and if the ptrl is writable, returns 0 other wise
// on an abort error, lock on the local root is released, then you only need to call delete_page_table_range_locker
int set_in_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, uint64_t page_id, const void* transaction_id, int* abort_error);

// finds bucket_id in page_table that is find_pos compared to the given bucket_id
// it will return the bucket_id (being an in-out parameter) and page_id
// if page_id == NULL_PAGE_ID, then no such bucket_id found
// on an abort error, lock on the local root is released, then you only need to call delete_page_table_range_locker
uint64_t find_non_NULL_PAGE_ID_in_page_table(page_table_range_locker* ptrl_p, uint64_t* bucket_id, find_position find_pos, const void* transaction_id, int* abort_error);

// deletes the page_table_range_locker, and releases lock on the local_root (if it is not NULL_persistent_page)
// a vaccum is required if the local_root is not the global root, it was write locked and it is empty
// if needs_vaccum is set, then you need to open a new iterator, in write locked mode on the WHOLE_BUCKET_RANGE and call vaccum
// not performing a vaccum will still keep your array_table logically consistent but it will have a bloat that you would not be able to fix
// if you are sure of not calling a vaccum, then you may pass vaccum parameters as NULLs
void delete_page_table_range_locker(page_table_range_locker* ptrl_p, uint64_t* vaccum_bucket_id, int* vaccum_needed, const void* transaction_id, int* abort_error);

// vaccum must be called with the local_root being the global root, in write locked mode -> this is so as to ensure that a range_locker initialized for vaccum does not create cascading vaccum calls
// this will check if pages corresponding the vaccum_bucket_id is empty, if so, it will discard all logically redundant pages
// on an abort error, lock on the local root is released, then you only need to call delete_array_table_range_locker
int perform_vaccum_page_table_range_locker(page_table_range_locker* ptrl_p, uint64_t vaccum_bucket_id, const void* transaction_id, int* abort_error);

#endif