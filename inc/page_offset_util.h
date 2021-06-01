#ifndef PAGE_OFFSET_UTIL_H
#define PAGE_OFFSET_UTIL_H

// pointer equivalent of NULL page, i.e. points to no page
#define NULL_PAGE_REF				(~0)

// page_type of any page of the page list
#define PAGE_LIST_PAGE_TYPE         0

// page references on any of the page of the page_list
// this must equal NEXT_SIBLING_PAGE_REF
#define NEXT_PAGE_REF               0

// two of the page types found inside a bplus tree
#define LEAF_PAGE_TYPE              1
#define INTERIOR_PAGE_TYPE          2

// page reference on interior page
// i.e. page reference to the sub-bplus_tree that contains all data that is lesser than the least key on this page
#define ALL_LEAST_VALUES_REF        0

// page references on leaf page
// this must equal NEXT_PAGE_REF
#define NEXT_SIBLING_PAGE_REF       NEXT_PAGE_REF

#endif