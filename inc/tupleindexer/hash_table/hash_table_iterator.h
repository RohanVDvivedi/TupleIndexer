#ifndef HASH_TABLE_ITERATOR_H
#define HASH_TABLE_ITERATOR_H

#include<tupleindexer/utils/bucket_range.h>

#include<tupleindexer/page_table/page_table_range_locker.h>
#include<tupleindexer/linked_page_list/linked_page_list_iterator.h>

#include<tupleindexer/hash_table/hash_table_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>
#include<tupleindexer/common/materialized_key.h>

#include<tupleindexer/hash_table/hash_table_handle.h>

typedef struct hash_table_iterator hash_table_iterator;
struct hash_table_iterator
{
	hash_table_handle* hth_p;

	// the key you should be looking for
	const void* key;
	materialized_key mat_key; // this is the materialized version of the key (in the previous line), it is valid only if key is provided, else it must be initialized to empty struct

	// range for locking the ptrl_p, only used when key == NULL
	bucket_range lock_range;

	// curr_bucket_id that lpli_p is pointing at
	uint64_t curr_bucket_id;

	// page_table_range_locker for the hash_table
	page_table_range_locker ptrl_mem;
	page_table_range_locker* ptrl_p;

	// linked_page_list_iterator pointing to the curr_bucket_id
	linked_page_list_iterator lpli_mem;
	linked_page_list_iterator* lpli_p;

	// any of ptrl_p and lpli_p can be NULL during the lifetime of the hash_table_iterator

	const hash_table_tuple_defs* httd_p;

	const page_access_methods* pam_p;

	const page_modification_methods* pmm_p;
	// for a read-only hash_table_iterator, pmm_p = NULL

	// bucket_count of the hash_table when this iterator was created
	uint64_t bucket_count;

	// for internal use, will be set if the iterator was malloc-ed on get_new
	unsigned int must_free_on_destroy : 1;
};

// creates a new hash_table_iterator for the given bucket range
// either provide key
// of if you do not provide key, then a subset of the lock_range (that actually exists, depending on the bucket_count) becomes iterable
// on abort_error, NULL is returned
hash_table_iterator* get_new_hash_table_iterator(hash_table_iterator* iter_mem, hash_table_handle* hth_p, bucket_range bucket_range, const void* key, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns NULL if hti_p is writable OR on an abort error
// on an abort_error, hti_p will still hold its locks
hash_table_iterator* clone_hash_table_iterator(hash_table_iterator* iter_mem, const hash_table_iterator* hti_p, const void* transaction_id, int* abort_error);

// return the bucket_count of the hash_table, at the instant when this iterator was created
// note: this may not be the actual bucket_count, if a expand_hash_table/shrink_hash_table was called post the initialization of this hash_table_iterator
uint64_t get_bucket_count_hash_table_iterator(const hash_table_iterator* hti_p);

// returns 1, if the iterator is writable
int is_writable_hash_table_iterator(const hash_table_iterator* hti_p);

// get the bucket that the iterator is pointiung to
uint64_t get_curr_bucket_index_for_hash_table_iterator(const hash_table_iterator* hti_p);

// check if the current bucket is empty (or NULL) or full
int is_curr_bucket_empty_for_hash_table_iterator(const hash_table_iterator* hti_p);
int is_curr_bucket_full_for_hash_table_iterator(const hash_table_iterator* hti_p);

// get the tuple that we are currenting pointing to
// returns NULL, if the hti_p->key != NULL, and key(curr_tuple) != hti_p->key
// the return value of this function is referred to as the curr_tuple of the iterator, for the remainder of the declarations in this file
const void* get_tuple_hash_table_iterator(const hash_table_iterator* hti_p);

typedef enum hash_table_iteration_constraint hash_table_iteration_constraint;
enum hash_table_iteration_constraint
{
	GO_NEXT_TUPLE_IN_SAME_BUCKET = 0,
	GO_NEXT_TUPLE_IN_MAY_BE_NEXT_BUCKET = 1,
	GO_NEXT_TUPLE_IN_MAY_BE_NEXT_EXISTING_BUCKET = 2
};

// jumps one tuple next
// if hti_p->key == NULL, then the iterator is allowed to jump buckets, if constr != GO_NEXT_TUPLE_IN_SAME_BUCKET
// on an abort_error, returns 0 and fails
int next_hash_table_iterator(hash_table_iterator* hti_p, hash_table_iteration_constraint constr, const void* transaction_id, int* abort_error);

// jumps one tuple prev
// if hti_p->key == NULL, then the iterator is allowed to jump buckets, if constr != GO_NEXT_TUPLE_IN_SAME_BUCKET
// on an abort_error, returns 0 and fails
int prev_hash_table_iterator(hash_table_iterator* hti_p, hash_table_iteration_constraint constr, const void* transaction_id, int* abort_error);

// for the below 4 functions curr_tuple refers to the return value of the get_tuple_hash_table_iterator()

// insertions allowed only if hti_p->key != NULL, and key(tuple) == hti_p->key
// on an abort_error, returns 0 and fails
int insert_in_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error);

// update allowed only if curr_tuple != NULL, and key(curr_tuple) == key(tuple i.e. the new tuple)
// on an abort_error, returns 0 and fails
int update_at_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error);

// remove allowed only if curr_tuple != NULL
// on an abort_error, returns 0 and fails
int remove_from_hash_table_iterator(hash_table_iterator* hti_p, const void* transaction_id, int* abort_error);

// update_non_key_element allowed only if curr_tuple != NULL
// update a non_key column inplace at the place that the hash_table_iterator is pointing to
// ADVISED 	:: only update columns that do not change the tuple size on the page, else the page may become less than half full and this can not be fixed by this function
//			:: also attempting to update to a element value that can increase the tuple size, may even fail, because the slot for the tuple is not big enough
// on an abort_error, returns 0 and fails
int update_non_key_element_in_place_at_hash_table_iterator(hash_table_iterator* hti_p, positional_accessor element_index, const datum* element_value, const void* transaction_id, int* abort_error);

#include<tupleindexer/hash_table/hash_table_vaccum_params.h>

void delete_hash_table_iterator(hash_table_iterator* hti_p, hash_table_vaccum_params* htvp, const void* transaction_id, int* abort_error);

#endif