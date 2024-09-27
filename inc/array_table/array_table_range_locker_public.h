#ifndef ARRAY_TABLE_RANGE_LOCKER_PUBLIC_H
#define ARRAY_TABLE_RANGE_LOCKER_PUBLIC_H

#include<bucket_range.h>
#include<persistent_page.h>
#include<find_position.h>

typedef struct array_table_range_locker array_table_range_locker;

// creates a new array_table_range_locker for the given range
// you want to call set then the pmm_p must not be NULL
// on abort_error, NULL is returned
array_table_range_locker* get_new_array_table_range_locker(uint64_t root_page_id, bucket_range lock_range, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns NULL if atrl_p is writable OR on an abort error
// on an abort_error, atrl_p will still hold its locks
array_table_range_locker* clone_array_table_range_locker(const array_table_range_locker* atrl_p, const void* transaction_id, int* abort_error);

// minimizes the lock range of the range_locker
// on an abort error, lock on the local root is released, then you only need to call delete_array_table_range_locker
int minimize_lock_range_for_array_table_range_locker(array_table_range_locker* atrl_p, bucket_range lock_range, const void* transaction_id, int* abort_error);

// get lock range for the array_table_range_locker
bucket_range get_lock_range_for_array_table_range_locker(const array_table_range_locker* atrl_p);

// check if the array_table_range_locker is locked for writing
// you may set only if this returns 1
int is_writable_array_table_range_locker(const array_table_range_locker* atrl_p);

// you may only get, if the bucket_id is within get_lock_range_for_array_table_range_locker()
// on an abort error, lock on the local root is released, then you only need to call delete_array_table_range_locker
const void* get_from_array_table(array_table_range_locker* atrl_p, uint64_t bucket_id, void* preallocated_memory, const void* transaction_id, int* abort_error);

// you may call set, if the bucket_id is within get_lock_range_for_array_table_range_locker() and if the atrl is writable, returns 0 other wise
// on an abort error, lock on the local root is released, then you only need to call delete_array_table_range_locker
int set_in_array_table(array_table_range_locker* atrl_p, uint64_t bucket_id, const void* record, const void* transaction_id, int* abort_error);

// finds bucket_id in array_table that is find_pos compared to the given bucket_id
// it will return the bucket_id (being an in-out parameter) and the record at that bucket_id
// if return value == NULL, then no such bucket_id, (with non NULL record) was found
// on an abort error, lock on the local root is released, then you only need to call delete_array_table_range_locker
const void* find_non_NULL_entry_in_array_table(array_table_range_locker* atrl_p, uint64_t* bucket_id, void* preallocated_memory, find_position find_pos, const void* transaction_id, int* abort_error);

// deletes the array_table_range_locker, and releases lock on the local_root (if it is not NULL_persistent_page)
// a vaccum is required if the local_root is not the global root, it was write locked and it is empty
// if needs_vaccum is set, then you need to open a new iterator, in write locked mode on the WHOLE_BUCKET_RANGE and call vaccum
// not performing a vaccum will still keep your array_table logically consistent but it will have a bloat that you would not be able to fix
void delete_array_table_range_locker(array_table_range_locker* atrl_p, uint32_t* vaccum_bucket_id, int* vaccum_needed, const void* transaction_id, int* abort_error);

#endif