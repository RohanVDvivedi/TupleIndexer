#ifndef BPLUS_TREE_STACKED_ITERATOR_CUSTOM_LOCKING_TYPE_H
#define BPLUS_TREE_STACKED_ITERATOR_CUSTOM_LOCKING_TYPE_H

// custom stacked iterator locking type, to only lock leaf pages with write lock, while lock interior pages with read locks
#define READ_LOCK_INTERIOR_WRITE_LOCK_LEAF 100

#endif