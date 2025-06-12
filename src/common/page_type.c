#include<tupleindexer/common/page_type.h>

char const * const page_type_string[] = {
	[BPLUS_TREE_LEAF_PAGE]     = "BPLUS_TREE_LEAF_PAGE",
	[BPLUS_TREE_INTERIOR_PAGE] = "BPLUS_TREE_INTERIOR_PAGE",
	[ARRAY_TABLE_PAGE]         = "ARRAY_TABLE_PAGE",
	[LINKED_PAGE_LIST_PAGE]    = "LINKED_PAGE_LIST_PAGE",
	[WORM_HEAD_PAGE]           = "WORM_HEAD_PAGE",
	[WORM_ANY_PAGE]            = "WORM_ANY_PAGE",
	[HEAP_PAGE]                = "HEAP_PAGE",
};