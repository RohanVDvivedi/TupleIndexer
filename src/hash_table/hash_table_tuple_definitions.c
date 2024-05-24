#include<hash_table_tuple_definitions.h>

#include<stdlib.h>
#include<string.h>

int init_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p, const page_access_specs* pas_p, const tuple_def* record_def, const uint32_t* key_element_ids, uint32_t key_element_count, uint64_t (*hash_func)(const void* data, uint32_t data_size))
{
	// basic parameter check
	if(key_element_count == 0 || key_element_ids == NULL || record_def == NULL || get_element_def_count_tuple_def(record_def) == 0)
		return 0;

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	httd_p->hash_func = hash_func;

	httd_p->key_element_count = key_element_count;

	httd_p->key_element_ids = malloc(sizeof(uint32_t) * httd_p->key_element_count);
	if(httd_p->key_element_ids == NULL) // memory allocation failed
		exit(-1);
	memcpy(httd_p->key_element_ids, key_element_ids, sizeof(uint32_t) * httd_p->key_element_count);

	if(!init_linked_page_list_tuple_definitions(&(httd_p->lpltd), pas_p, record_def))
	{
		deinit_hash_table_tuple_definitions(httd_p);
		return 0;
	}

	if(!init_page_table_tuple_definitions(&(httd_p->pttd), pas_p))
	{
		deinit_hash_table_tuple_definitions(httd_p);
		return 0;
	}

	return 1;
}

int check_if_record_can_be_inserted_for_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple)
{
	if(record_tuple == NULL)
		return 0;

	return check_if_record_can_be_inserted_for_linked_page_list_tuple_definitions(&(httd_p->lpltd), record_tuple);
}

int extract_key_from_record_tuple_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple, void* key)
{
	// init the key tuple
	init_tuple(httd_p->key_def, key);

	// copy all the elements from record into the key tuple
	int res = 1;
	for(uint32_t i = 0; i < httd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(httd_p->key_def, i, key, httd_p->lpltd.record_def, httd_p->key_element_ids[i], record_tuple);

	return res;
}

int64_t get_floor_log_base_2(uint64_t x)
{
	int64_t l = 0;
	int64_t h = 63;
	int64_t res = 64; // an invalid value for 64 bit number's floor(log(x)/log(2))
	while(l <= h)
	{
		int64_t m = l + (h - l) / 2;
		uint64_t power = UINT64_C(1) << m;
		if(x >= power)
		{
			res = m;
			l = m + 1;
		}
		else
			h = m - 1;
	}
	return res;
}

uint64_t get_hash_table_split_index(uint64_t bucket_count)
{
	return bucket_count % (UINT64_C(1) << get_floor_log_base_2(bucket_count));
}

uint64_t get_hash_value_for_key_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* key)
{
	return hash_tuple(key, httd_p->key_def, NULL, httd_p->hash_func, httd_p->key_element_count);
}

uint64_t get_hash_value_for_record_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple)
{
	return hash_tuple(record_tuple, httd_p->lpltd.record_def, httd_p->key_element_ids, httd_p->hash_func, httd_p->key_element_count);
}

void deinit_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p)
{
	if(httd_p->key_element_ids)
		free(httd_p->key_element_ids);
	if(httd_p->key_def)
		delete_tuple_def(httd_p->key_def);

	deinit_linked_page_list_tuple_definitions(&(httd_p->lpltd));

	deinit_page_table_tuple_definitions(&(httd_p->pttd));
}

void print_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p)
{
	printf("Hash_table tuple defs:\n");

	printf("key_element_count = %"PRIu32"\n", httd_p->key_element_count);

	printf("key_element_ids = ");
	if(httd_p->key_element_ids)
	{
		printf("{ ");
		for(uint32_t i = 0; i < httd_p->key_element_count; i++)
			printf("%u, ", httd_p->key_element_ids[i]);
		printf(" }\n");
	}
	else
		printf("NULL\n");

	printf("key_def = ");
	if(httd_p->key_def)
		print_tuple_def(httd_p->key_def);
	else
		printf("NULL\n");

	print_linked_page_list_tuple_definitions(&(httd_p->lpltd));

	print_page_table_tuple_definitions(&(httd_p->pttd));
}