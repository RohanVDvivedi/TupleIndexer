#include<tupleindexer/common/materialized_key.h>

#include<stdlib.h>

materialized_key materialize_key_from_tuple(const void* tuple, const tuple_def* tpl_d, const positional_accessor* key_columns_to_materialize, uint32_t key_element_count)
{
	materialized_key mat_key = {};

	mat_key.key_element_count = key_element_count;

	mat_key.key_dtis = malloc(sizeof(data_type_info*) * key_element_count);
	if(mat_key.key_dtis == NULL)
		exit(-1);

	mat_key.keys = malloc(sizeof(user_value) * key_element_count);
	if(mat_key.keys == NULL)
		exit(-1);

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		mat_key.key_dtis[i] = get_type_info_for_element_from_tuple_def(tpl_d, ((key_columns_to_materialize == NULL) ? STATIC_POSITION(i) : (key_columns_to_materialize[i])));
		get_value_from_element_from_tuple(&(mat_key.keys[i]), tpl_d, ((key_columns_to_materialize == NULL) ? STATIC_POSITION(i) : (key_columns_to_materialize[i])), tuple);
	}

	return mat_key;
}

void destroy_materialized_key(materialized_key* mat_key)
{
	mat_key->key_element_count = 0;
	if(mat_key->key_dtis != NULL)
		free((void*)(mat_key->key_dtis));
	if(mat_key->keys != NULL)
		free((void*)(mat_key->keys));
}