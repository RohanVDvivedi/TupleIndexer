#ifndef HEAP_TABLE_H
#define HEAP_TABLE_H

/*
	heap_table is a derivative data structure built entirely on top of bplus_tree
	it is only usefull to perform free space management and tracking all pages in a heap_table (concept similar to postgresql)
	heap_table only manages heap_page-s by storing their (unsed_space, page_id) entry in a bplus_tree
	to find the best-fit to perform an insert into an existing heap_page
	ideally the unused_space in the heap_table is a hint and not a source of truth, i.e. it has to be eventually consistent, else you encounter redundant write amplification

	as expected the (page_id, tuple_index) in heap_page-s of heap_table are meant to be fixed fixed for the life span of the tuple, thus allowing us to store (page_id, tuple_index) in external indexes (b+tree or hash_table)
	and this (page_id, tuple_index) can be treated as a pointer to the tuple
	also you do not need to lock an scan the heap_table to access a heap_page, instead you just need to directly lock the heap_page and access the tuple, or possibly modify it
	but please ensure to eventually fix the unused_space entry in the heap_table AFTER THE HEAP_PAGE LOCK HAS BEEN RELEASED synchronously or asynchronously

	additionally you can also use the heap_table_iterator to iterate over all the heap_page-s and scan all the tuples, but please be sure that fix_* and track_* functions may reorder heap_pages
	requiring you to take a lock in shared mode for a complete scan and in exclusive mode to fix_* unused_space entries
*/

#endif