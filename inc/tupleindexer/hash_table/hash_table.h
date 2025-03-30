#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include<hash_table_tuple_definitions_public.h>

#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

/*
	A hash_table is just a page_table where each of its buckets point to a head page of a linked_page_list OR a NULL_PAGE_ID if it is empty.
	It is managed as a linear hashing algorithm (a technique for dynamic hashing).

	self defined constraint -> hash_table->page_table[bucket_count] = root_page_id

	The above to techniques allow us to work concurrently with different buckets.
	And it allows us to expand or shrink the hash_table (with linear hashing technique) concurrently, with other buckets being actively accessed.
*/

// returns pointer to the root page of the newly created hash_table
uint64_t get_new_hash_table(uint64_t initial_bucket_count, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// below three functions access/modify the bucket_cout of the hash_table, and must be called exclusively
// i.e. concurrent shrink and expand calls are allowed but the result is not deterministic, the result will depend on who gets the lock first
// hence I will suggest have one lock type for accessing modifying the bucket_count for the hash_table

// returns the current bucket count of the hash_table
// note: this is the instantaneous bucket_count of the hash_table
uint64_t get_bucket_count_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// increments the bucket_count of the hash_table by 1
// it fails on an abort error OR if the bucket_count == UINT64_MAX
int expand_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// decrements the bucket_count of the hash_table by 1
// it fails on an abort error OR if the bucket_count == UINT64_MAX
int shrink_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// frees all the pages occupied by the hash_table
// it may fail on an abort_error, ALSO you must ensure that you are the only one who has lock on the given hash_table
int destroy_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// prints all the pages in the hash_table
// it may return an abort_error, unable to print all of the hash_table pages
void print_hash_table(uint64_t root_page_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#include<hash_table_iterator_public.h>

#include<hash_table_vaccum_params.h>

// performs hash_table vaccum
// returns 1 on success
// htvp is an array of hash_table_vaccum_params of length vaccum_params_count, it can be used to batch vaccum
int perform_vaccum_hash_table(uint64_t root_page_id, const hash_table_vaccum_params* htvp, uint32_t params_count, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

#endif