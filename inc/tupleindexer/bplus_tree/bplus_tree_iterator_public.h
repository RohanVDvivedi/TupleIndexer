#ifndef BPLUS_TREE_ITERATOR_PUBLIC_H
#define BPLUS_TREE_ITERATOR_PUBLIC_H

// this iterator can only be used to reading leaf tuples of the b+tree

// custom stacked iterator locking type, to only lock leaf pages with write lock, while lock interior pages with read locks
#include<tupleindexer/bplus_tree/bplus_tree_walk_down_custom_lock_type.h>
#include<tupleindexer/common/find_position.h>

typedef struct bplus_tree_iterator bplus_tree_iterator;

// returns NULL if bpi_p is writable OR on an abort error
// on an abort_error, bpi_p will still hold its locks
bplus_tree_iterator* clone_bplus_tree_iterator(const bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// returns true, if the leaf page of the bplus_tree_iterator is/will be write locked
// does not say anything about the locks on the interior pages, you need to remember the lock_type if this is a stacked iterator
int is_writable_bplus_tree_iterator(const bplus_tree_iterator* bpi_p);

// returns true, if the path from the root ot leaf is locked by the iterator and is used for iteration instead of the next and prev linkages of the leaf page
int is_stacked_bplus_tree_iterator(const bplus_tree_iterator* bpi_p);

// check if the bplus_tree is empty or not
int is_empty_bplus_tree(const bplus_tree_iterator* bpi_p);

int is_beyond_min_tuple_bplus_tree_iterator(const bplus_tree_iterator* bpi_p);
int is_beyond_max_tuple_bplus_tree_iterator(const bplus_tree_iterator* bpi_p);

// below function can be used to test if you are exactly at the minimum or maximum tuple in the bplus_tree_iterator
int is_at_min_tuple_bplus_tree_iterator(const bplus_tree_iterator* bpi_p);
int is_at_max_tuple_bplus_tree_iterator(const bplus_tree_iterator* bpi_p);

// it moves the cursor forward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
// on an abort_error, all the lps pages will be unlocked by the bplus_tree_iterator
int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// returns pointer to the current tuple that the cursor points to
// it returns NULL,
// 	* case 1 when the page that it points to is empty (0 tuples)
// 	* case 2 when the bplus_tree_iterator has reached the end
// the pointer to the tuple returned by this function is valid only until next_*, prev_*, remove_from_*, update_at_* and delete_* functions are not called
const void* get_tuple_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// it moves the cursor backward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
// on an abort_error, all the lps pages will be unlocked by the bplus_tree_iterator
int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// all locks held by the iterator will be released before, the iterator is destroyed/deleted
// releasing locks may result in an abort_error
void delete_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// debug function
void debug_print_lock_stack_for_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

#include<stdint.h>

// works only on stacked iterator, with lock_type = READ_LOCK and READ_LOCK_INTERIOR_WRITE_LOCK_LEAF, else it will fail with a 0
// here f_pos1 = MIN, refers to the -infinity, and f_pos2 = MAX refers to +infiniy
int narrow_down_range_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* key1, find_position f_pos1, const void* key2, find_position f_pos2, uint32_t key_element_count_concerned, const void* transaction_id, int* abort_error);

// below functions can be used with only writable iterator

typedef enum bplus_tree_after_remove_operation bplus_tree_after_remove_operation;
enum bplus_tree_after_remove_operation
{
	GO_NEXT_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION = 0,
	GO_PREV_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION = 1,
	PREPARE_FOR_DELETE_ITERATOR_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION = 2,
};

// if you pass prepare_for_delete_iterator_on_success = 1 OR aft_op = PREPARE_FOR_DELETE_ITERATOR_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION
// then upon success the only operation you need to and can do is delete_bplus_tree_iterator
// upon an abort_error, you obviously can do is delete_bplus_tree_iterator
// while on a failure without abort error, you can do what ever you want next, the iterator would just have been almost untouched

// remove the tuple that the bplus_tree_iterator is currently pointing at
// works only on a stacked iterator with lock_type = WRITE_LOCK
// on an abort error, all locks are released, then you only need to call delete_bplus_tree_iterator
int remove_from_bplus_tree_iterator(bplus_tree_iterator* bpi_p, bplus_tree_after_remove_operation aft_op, const void* transaction_id, int* abort_error);

// update the curr_tuple being pointed at by the bplus_tree_iterator with the parameter tuple
// works only on a stacked iterator with lock_type = WRITE_LOCK
// it will also work on stacked_iterator with lock_type = READ_LOCK_INTERIOR_WRITE_LOCK_LEAF or a unstacked_iterator with lock_type = WRITE_LOCK, if and only if the tuple to be updated is exactly equal to the new tuple in size
// on an abort error, all locks are released, then you only need to call delete_bplus_tree_iterator
// here the prepare_for_delete_iterator_on_success flag only works for stacked WRITE_LOCKed iterator case only
int update_at_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* tuple, int prepare_for_delete_iterator_on_success, const void* transaction_id, int* abort_error);

// insert in the bplus_tree_iterator with the parameter tuple
// works only on a stacked iterator with lock_type = WRITE_LOCK
// on an abort error, all locks are released, then you only need to call delete_bplus_tree_iterator
// on success, if prepare_for_delete_iterator_on_success = 1, then the all lockes held by the iterator are released, else it is made to point to the tuple inserted
// this function may fail if the rightful position of the tuple to be inserted is not being pointed at currently by the iterator, i.e. the iterator's position is not correct for the insertion of the given tuple
// for this reason only use it to insert a new MIN tuple while pointing at MIN or beyond MIN tuples, and like wise for the MAX tuple, i.e. it is designed to work closely with the auto increment provided by most databases
int insert_using_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* tuple, int prepare_for_delete_iterator_on_success, const void* transaction_id, int* abort_error);

// you can use the below function only to update only a fixed length non-key column

#include<tuplestore/user_value.h>
#include<tuplestore/tuple.h>

// update a non_key column inplace at the place that the bplus_tree_iterator is pointing to
// ADVISED 	:: only update columns that do not change the tuple size on the page, else the page may become less than half full and this can not be fixed without a merge, and you can not mrege with an iterator
//			:: also attempting to update to a element value that can increase the tuple size, may even fail, because the slot for the tuple is not big enough
// on an abort_error, all the lps pages will be unlocked by the bplus_tree_iterator
int update_non_key_element_in_place_at_bplus_tree_iterator(bplus_tree_iterator* bpi_p, positional_accessor element_index, const user_value* element_value, const void* transaction_id, int* abort_error);

#endif