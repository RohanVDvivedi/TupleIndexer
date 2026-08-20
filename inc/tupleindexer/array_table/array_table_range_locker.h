#ifndef ARRAY_TABLE_RANGE_LOCKER_H
#define ARRAY_TABLE_RANGE_LOCKER_H

#include<tupleindexer/array_table/array_table.h>
#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/array_table/array_table_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

#include<tupleindexer/utils/bucket_range.h>
#include<tupleindexer/common/find_position.h>

typedef struct array_table_range_locker array_table_range_locker;
struct array_table_range_locker
{
	// the range that this range_locker locks
	// this is the delegated range of local_root, delegated to it by its parent
	// for the root of the array_table this value equals [0, UINT64_MAX] (i.e. WHOLE_BUCKET_RANGE), both inclusive
	bucket_range delegated_local_root_range;

	// maximum level that this local root may reach
	// this is equivalent to (local_root.parent.level - 1)
	// for the root of the page_table this value is equal to (attd_p->max_page_table_height-1)
	uint32_t max_local_root_level;

	// local root of the range_locker, this page stays locked until you destroy the range_locker, unless in case of an abort
	// you can only set a bucket_id if the lock_type of local_root is WRITE_LOCK
	persistent_page local_root;

	// actual page_id of the root of the page_table, that we would be working with
	uint64_t root_page_id;

	const array_table_tuple_defs* attd_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
	// for a read-only array_table_range_locker, pmm_p = NULL

	// for internal use, will be set if the iterator was malloc-ed on get_new
	unsigned int must_free_on_destroy : 1;
};

// creates a new array_table_range_locker for the given range
// you want to call set then the pmm_p must not be NULL
// on abort_error, NULL is returned
array_table_range_locker* get_new_array_table_range_locker(array_table_range_locker* iter_mem, uint64_t root_page_id, bucket_range lock_range, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns NULL if atrl_p is writable OR on an abort error
// on an abort_error, atrl_p will still hold its locks
array_table_range_locker* clone_array_table_range_locker(array_table_range_locker* iter_mem, const array_table_range_locker* atrl_p, const void* transaction_id, int* abort_error);

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
// if you are sure of not calling a vaccum, then you may pass vaccum parameters as NULLs
void delete_array_table_range_locker(array_table_range_locker* atrl_p, uint64_t* vaccum_bucket_id, int* vaccum_needed, const void* transaction_id, int* abort_error);

// vaccum must be called with the local_root being the global root, in write locked mode -> this is so as to ensure that a range_locker initialized for vaccum does not create cascading vaccum calls
// this will check if pages corresponding the vaccum_bucket_id is empty, if so, it will discard all logically redundant pages
// on an abort error, lock on the local root is released, then you only need to call delete_array_table_range_locker
int perform_vaccum_array_table_range_locker(array_table_range_locker* atrl_p, uint64_t vaccum_bucket_id, const void* transaction_id, int* abort_error);

#endif