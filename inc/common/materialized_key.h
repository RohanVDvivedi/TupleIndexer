#ifndef MATERIALIZED_KEY_H
#define MATERIALIZED_KEY_H

#include<tuple.h>

/*
	You mught encounter cases where you need to compare the same key with a large number of tuples on the data structure like bplus_tree and hash_table
	You may instead materialize the key in such cases and perform the search to speed it up

	Before you materialie the key from the tuple, you must ensure that all the key elements in the tuple accessible
*/

typedef struct materialized_key materialized_key;
struct materialized_key
{
	uint32_t key_element_count;

	data_type_info const ** key_dtis;

	user_value* keys;
};

materialized_key materialize_key_from_tuple(const void* tuple, const tuple_def* tpl_d, const positional_accessor* key_columns_to_materialize, uint32_t key_element_count);

void destroy_materialized_key(materialized_key* mat_key);

#endif