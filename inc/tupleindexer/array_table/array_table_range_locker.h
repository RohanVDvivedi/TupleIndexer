#ifndef ARRAY_TABLE_RANGE_LOCKER_H
#define ARRAY_TABLE_RANGE_LOCKER_H

#include<array_table.h>
#include<persistent_page.h>
#include<array_table_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

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
};

// all functions on array_table_range_locker are declared here, in this header file
#include<array_table_range_locker_public.h>

#endif