#ifndef HEAP_TABLE_ITERATOR_H
#define HEAP_TABLE_ITERATOR_H

#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<tupleindexer/heap_table/heap_table_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_access_methods.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>

/*
	This is a read-only, leaf-only, forward-only bplus_tree iterator over the heap_table
	But you can request a lock on the heap_page in a read or even write locked mode
	If you have tuples you want to insert during this scan, you possibly need to close this scan and open a new scan, find a right best-fit heap_page and insert into it
	relatch and start scanning from the previous heap_page-s unused_space and page_id

	if your architecture and logic permits and there is enough space on the heap_page, you can modify it right here in this scan

	if you want to prevent heap_table restructuring during this whole time you probably need a shared/exclusive lock to prevent this (by letting fix_* and track_* functions accesses in exclusive mode only)
*/

typedef struct heap_table_iterator heap_table_iterator;
struct heap_table_iterator
{
	uint64_t root_page_id;

	// actual iterator over the heap_table's bplus_tree
	// allows only read-only, leaf-only and forward only accesses
	bplus_tree_iterator* bpi_p;

	const heap_table_tuple_defs* httd_p;

	const page_access_methods* pam_p;
};

// all functions on heap_table_iterator are declared here, in this header file
#include<tupleindexer/heap_table/heap_table_iterator_public.h>

#endif