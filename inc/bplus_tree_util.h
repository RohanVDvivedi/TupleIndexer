#ifndef BPLUS_TREE_UTIL_H
#define BPLUS_TREE_UTIL_H

#define LEAF_PAGE_TYPE 0
#define INTERIOR_PAGE_TYPE 1

// page reference on interior page
// i.e. page reference to the sub-bplus_tree that contains all data lesser than the least key on this page
#define ALL_LEAST_REF 0

// page references on leaf page
#define NEXT_PAGE_REF 0
#define PREV_PAGE_REF 1

#endif