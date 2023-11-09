#ifndef BPLUS_TREE_ITERATOR_PUBLIC_H
#define BPLUS_TREE_ITERATOR_PIBLIC_H

// this iterator can only be used to reading leaf tuples of the b+tree

typedef struct bplus_tree_iterator bplus_tree_iterator;

// returns true, if the leaf page of the bplus_tree_iterator is/will be write locked
int is_writable_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// returns true, if the path from the root ot leaf is locked by the iterator and is used for iteration instead of the next and prev linkages of the leaf page
int is_stacked_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// it moves the cursor forward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
// on an abort_error, all the lps pages will be unlocked by the bplus_tree_iterator
int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// returns pointer to the current tuple that the cursor points to
// it returns NULL,
// 	* case 1 when the page that it points to is empty (0 tuples),
//			** this must not happen in a bplus_tree, except when the bplus_tree is empty (contains no records), this will never happen to you, if you are using only the public api functions
// 	* case 2 when the bplus_tree_iterator has reached the end
// the pointer to the tuple returned by this function is valid only until next_*, prev_* and delete_* functions are not called
const void* get_tuple_bplus_tree_iterator(bplus_tree_iterator* bpi_p);

// it moves the cursor backward by a tuple
// returns 1 for success, it returns 0, if there are no records to move to
// on an abort_error, all the lps pages will be unlocked by the bplus_tree_iterator
int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// all locks held by the iterator will be released before, the iterator is destroyed/deleted
// releasing locks may result in an abort_error
void delete_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

// below functions can be used with only writable iterator
// you can use it to update only a fixed length non-key column

// API TODO

#endif