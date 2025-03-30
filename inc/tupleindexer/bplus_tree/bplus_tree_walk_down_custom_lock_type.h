#ifndef BPLUS_TREE_WALK_DOWN_CUSTOM_LOCK_TYPE_H
#define BPLUS_TREE_WALK_DOWN_CUSTOM_LOCK_TYPE_H

#define READ_LOCK_INTERIOR_WRITE_LOCK_LEAF 100
// this can be passed to find_in_bplus_tree as lock_type. with is_stacked = 1, if you want an iterator for which its interior pages are locked with read lock and leaf pages with write lock

#endif