#ifndef HASH_TABLE_TUPLE_DEFINITIONS_H
#define HASH_TABLE_TUPLE_DEFINITIONS_H

#include<tuple.h>
#include<inttypes.h>

#include<linked_page_list_tuple_definitions.h>
#include<page_table_tuple_definitions.h>

typedef struct hash_table_tuple_definitions hash_table_tuple_definitions;
struct hash_table_tuple_definitions
{
	// number of elements considered as keys
	uint32_t key_element_count;

	// element ids of the keys (as per their element_ids in lpltd.record_def)
	uint32_t* key_element_ids;

	// tuple definition of the key to be used with this bplus_tree
	// for all of find, insert, update and delete functionalities
	tuple_def* key_def;

	// tuple_definiton for the buckets of the hash_table
	linked_page_list_tuple_defs lpltd;

	// tuple_definition for the bucket-pointers of the hash_table
	page_table_tuple_defs pttd;
};

#endif