#ifndef HASH_TABLE_ITERATOR_H
#define HASH_TABLE_ITERATOR_H

#include<page_table_range_locker.h>
#include<linked_page_list_iterator.h>

#include<hash_table_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

typedef struct hash_table_iterator hash_table_iterator;
struct hash_table_iterator
{
	uint64_t root_page_id;

	// the key you should be looking for
	const void* key;

	// range for locking the ptrl_p
	// if this range has only 1 bucket, then ptrl_p may be NULL
	bucket_range bucket_range;

	// curr_bucket_id that lpli_p is pointing at
	uint64_t curr_bucket_id;

	// page_table_range_locker for the hash_table
	page_table_range_locker* ptrl_p;

	// linked_page_list_iterator pointing to the curr_bucket_id
	linked_page_list_iterator* lpli_p;

	// any of ptrl_p and lpli_p can be NULL during the lifetime of the 

	const hash_table_tuple_defs* httd_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
	// for a read-only hash_table_iterator, pmm_p = NULL

	// bucket_count of the hash_table when this iterator was created
	uint64_t bucket_count;
};

// all functions on hash_table_iterator are declared here, in this header file
#include<hash_table_iterator_public.h>

#endif