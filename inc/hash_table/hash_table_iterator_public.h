#ifndef HASH_TABLE_ITERATOR_PUBLIC_H
#define HASH_TABLE_ITERATOR_PUBLIC_H

#include<bucket_range.h>

typedef struct hash_table_iterator hash_table_iterator;

// creates a new hash_table_iterator for the given bucket range
// either provide key
// of if you do not provide key, then a subset of the lock_range (that actually exists, depending on the bucket_count) becomes iterable
// on abort_error, NULL is returned
hash_table_iterator* get_new_hash_table_iterator(uint64_t root_page_id, bucket_range bucket_range, const void* key, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// returns NULL if hti_p is writable OR on an abort error
// on an abort_error, hti_p will still hold its locks
hash_table_iterator* clone_hash_table_iterator(const hash_table_iterator* hti_p, const void* transaction_id, int* abort_error);

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

// jumps one tuple next
// if hti_p->key == NULL, then the iterator is allowed to jump buckets, if can_jump_bucket = 1
// on an ABORT_ERROR, all iterators that hash_table_iterator points to are deleted
int next_hash_table_iterator(hash_table_iterator* hti_p, int can_jump_bucket, const void* transaction_id, int* abort_error);

// jumps one tuple prev
// if hti_p->key == NULL, then the iterator is allowed to jump buckets, if can_jump_bucket = 1
// on an ABORT_ERROR, all iterators that hash_table_iterator points to are deleted
int prev_hash_table_iterator(hash_table_iterator* hti_p, int can_jump_bucket, const void* transaction_id, int* abort_error);

// for the below 4 functions curr_tuple refers to the return value of the get_tuple_hash_table_iterator()

// insertions allowed only if hti_p->key != NULL, and key(tuple) == hti_p->key
// on an ABORT_ERROR, all iterators that hash_table_iterator points to are deleted
int insert_in_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error);

// update allowed only if curr_tuple != NULL, and key(curr_tuple) == key(tuple i.e. the new tuple)
// on an ABORT_ERROR, all iterators that hash_table_iterator points to are deleted
int update_at_hash_table_iterator(hash_table_iterator* hti_p, const void* tuple, const void* transaction_id, int* abort_error);

// remove allowed only if curr_tuple != NULL
// on an ABORT_ERROR, all iterators that hash_table_iterator points to are deleted
int remove_from_hash_table_iterator(hash_table_iterator* hti_p, const void* transaction_id, int* abort_error);

// update_non_key_element allowed only if curr_tuple != NULL
// update a non_key column inplace at the place that the hash_table_iterator is pointing to
// ADVISED 	:: only update columns that do not change the tuple size on the page, else the page may become less than half full and this can not be fixed by this function
//			:: also attempting to update to a element value that can increase the tuple size, may even fail, because the slot for the tuple is not big enough
// on an ABORT_ERROR, all iterators that hash_table_iterator points to are deleted
int update_non_key_element_in_place_at_hash_table_iterator(hash_table_iterator* hti_p, positional_accessor element_index, const user_value* element_value, const void* transaction_id, int* abort_error);

#include<hash_table_vaccum_params.h>

void delete_hash_table_iterator(hash_table_iterator* hti_p, hash_table_vaccum_params* htvp, const void* transaction_id, int* abort_error);

#endif