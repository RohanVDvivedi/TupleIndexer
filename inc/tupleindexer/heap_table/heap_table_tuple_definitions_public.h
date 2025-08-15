#ifndef HEAP_TABLE_TUPLE_DEFINITIONS_PUBLIC_H
#define HEAP_TABLE_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuplestore/tuple.h>

#include<tupleindexer/bplus_tree/bplus_tree_tuple_definitions_public.h>

typedef struct heap_table_tuple_defs heap_table_tuple_defs;
struct heap_table_tuple_defs
{
	// specification of all the pages in the heap_table and the heap_page-s it contains
	const page_access_specs* pas_p;

	// tuple definition of the record on the heap_page-s of this heap_table
	tuple_def* record_def;

	// tuple_definiton for the bplus_tree of the heap_table
	bplus_tree_tuple_defs bpttd;

	// tuple_def for the (unused_space, page_id) records of the heap_table's bplus_tree
	tuple_def* entry_def;
};

// initializes the attributes in heap_table_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it relies on bpttd for most of its fnctionality
// returns 1 for success, it fails with 0
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_hash_table_tuple_definitions(heap_table_tuple_defs* httd_p, const page_access_specs* pas_p, const tuple_def* record_def);

// then resets all the heap_table_tuple_defs struct attributes to NULL or 0
void deinit_heap_table_tuple_definitions(heap_table_tuple_defs* httd_p);

// print heap_table_tuple_definitions
void print_heap_table_tuple_definitions(heap_table_tuple_defs* httd_p);

#endif