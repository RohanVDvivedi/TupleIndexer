#ifndef BPLUS_TREE_DEADLOCK_AVOIDING_LOCK_COMPATIBILITY_MATRIX_H
#define BPLUS_TREE_DEADLOCK_AVOIDING_LOCK_COMPATIBILITY_MATRIX_H

/*
	This file provides a lock compatibility matrix to avoid deadlock using an external glock (defined and implemented in LockKing library)
	This matrix must be used to initialize a glock, which should be held in respective mode to avoid deadlocks while working with a bplus_tree
	This only works when you actually have page level lock, this prevents your page level locks (ideally latches) from deadlocking
	Note::
		1. This deadlocks primarily arise from leaf only un-stacked iterators iterating in opposite directions
		2. Deadlocks can be entirely avoided and you may not need this glock, if you avoid leaf only un-stacked iterator in backward direction (yes backward leaf only iterators are the main cause)
		3. If your data_access_methods are designed to not have any page level locks, then you must atleast rely on a more generic rwlock (again, defined and implemented in LockKing), but this will give you less concurrency.
*/

// the first operation type is for direct, without iterator, insert, delete and update
// they do not have locks on all pages from root to leaf, and so these operations can allow higher concurrency
// but with a caveat that momentarily they may try to acquire lock on the next leaf of the current leaf (already held) and so may deadlock with leaf only traversals in backward direction
#define BPLUS_TREE_POINT_OPERATION 0

// following are the 4 leaf only traversals
// these 2 out of any 4, deadlock with each other when atleast 1 is a write iterator and both of them are iterating in opposite directions of each other (i.e. 1 is forward another is backward)
#define BPLUS_TREE_LEAF_ONLY_FORWARD_READ_ITERATOR   1
#define BPLUS_TREE_LEAF_ONLY_BACKWARD_READ_ITERATOR  2
#define BPLUS_TREE_LEAF_ONLY_FORWARD_WRITE_ITERATOR  3
#define BPLUS_TREE_LEAF_ONLY_BACKWARD_WRITE_ITERATOR 4

// The last operation is using the stacked iterator
// Since they hold locks on all pages from root to leaf, and for iterating forward or backward, release lock on the leaf and then reacquire lock on the next or previous sibling page using the data in the parent page, they never deadlock during iteration
// Even the write iterators when performing insert, delete or update hold all locks from root to leaf, make the access to the bplus_tree fully exclusive and avoid deadlocks
// they limit concurrency compared to leaf only traversals, but do not suffer from deadlock, due to varied reason explained above
#define BPLUS_TREE_STACKED_ITERATOR_OPERATIONS 5

#include<lockking/glock.h>

extern const glock_matrix bplus_tree_deadlock_avoidance_lock_matrix;

/*
	As discussed earlier
	1. Do not use this lock when there are no page level locks provided by your data_access_methods, in that particular case you need a standard reader-writer lock, similar to the one provided in LockKing library
	2. You can avoid using this lock entirely by using stacked iterators for backwards scanning, instead of leaf only iterators for backwards scanning, as they are the main source of deadlocks
*/

#endif