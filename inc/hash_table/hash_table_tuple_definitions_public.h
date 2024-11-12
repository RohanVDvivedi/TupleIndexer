#ifndef HASH_TABLE_TUPLE_DEFINITIONS_PUBLIC_H
#define HASH_TABLE_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuple.h>
#include<inttypes.h>

#include<linked_page_list_tuple_definitions_public.h>
#include<page_table_tuple_definitions_public.h>

typedef struct hash_table_tuple_defs hash_table_tuple_defs;
struct hash_table_tuple_defs
{
	// number of elements considered as keys
	uint32_t key_element_count;

	// element ids of the keys (as per their element_ids in lpltd.record_def)
	const positional_accessor* key_element_ids;

	// tuple definition of the key to be used with this bplus_tree
	// for all of find, insert, update and delete functionalities
	// shallow tuple_def with containees from the record_def
	tuple_def* key_def;

	// initial value of the hasher to hash tuples
	tuple_hasher hasher;

	// tuple_definiton for the buckets of the hash_table
	linked_page_list_tuple_defs lpltd;

	// tuple_definition for the bucket-pointers of the hash_table
	page_table_tuple_defs pttd;
};

// initializes the attributes in bplus_tree_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it allocates memory only for key_element_ids and key_def
// it relies on lpltd and pttd for most of its fnctionality
// returns 1 for success, it fails with 0, if the record_def has element_count 0 OR key_element_count == 0 OR key_element_ids == NULL OR if any of the key_element_ids is out of bounds
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p, const page_access_specs* pas_p, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count, const tuple_hasher* hasher);

// checks to see if a record_tuple can be inserted into a hash_table
// note :: you can not insert a NULL record in hash_table
int check_if_record_can_be_inserted_for_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple);

// copy all the key elements from the record_tuple to make the key
int extract_key_from_record_tuple_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple, void* key);

// get hash value for key, using the hash_table tuple defs
uint64_t get_hash_value_for_key_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* key);

// get hash value for record, using the hash_table tuple defs
uint64_t get_hash_value_for_record_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple);

// get bucket_index for key, using the hash_table tuple defs
uint64_t get_bucket_index_for_key_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* key, uint64_t bucket_count);

// get bucket_index for record, using the hash_table tuple defs
uint64_t get_bucket_index_for_record_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple, uint64_t bucket_count);

// it deallocates the key_element_ids, key_def, lpltd and pttd
// then resets all the hash_table_tuple_defs struct attributes to NULL or 0
void deinit_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p);

// print hash_table_tuple_definitions
void print_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p);

#endif