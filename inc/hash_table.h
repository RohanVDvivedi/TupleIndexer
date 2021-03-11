#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include<stdint.h>

#include<tuple.h>

#include<data_access_methods.h>

typedef enum hash_bucket_type hash_bucket_type;
enum hash_bucket_type
{
	PAGE_LISTS,
	BPLUS_TREES,
};

typedef struct hash_table_index hash_table_index;
struct hash_table_index
{
	// each bucket of the hash table index
	// is either a page_list or a bplus_tree
	hash_bucket_type type;

	// the page where the page_ids of all the bucket pages is written
	uint32_t root_page;

	// the tuple definition of the data elements of the page
	const tuple_def* tpl_d;

	// the number of elements from the beginning, that will be considered for comparison and hashing
	// i.e. the prefix elements of the tuple will be considered as key, and the rest will be considered as value
	uint16_t key_elements_count;
};

const void* find_in_hash_table_index(const hash_table_index* hti_p, const void* key_tuple, const data_access_methods* dam_p);

int insert_in_hash_table_index(hash_table_index* hti_p, const void* key_value_tuple, const data_access_methods* dam_p);

int delete_in_hash_table_index(hash_table_index* hti_p, const void* key_tuple, const data_access_methods* dam_p);

#endif