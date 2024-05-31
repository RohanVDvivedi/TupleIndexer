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

	// initialize key def

	// allocate memory for key_def and initialize it
	httd_p->key_def = get_new_tuple_def("temp_key_def", key_element_count, pas_p->page_size);
	if(httd_p->key_def == NULL) // memory allocation failed
		exit(-1);

	// result of inserting element_definitions to key_def
	int res = 1;

	// initialize element_defs of the key_def
	for(uint32_t i = 0; i < key_element_count && res == 1; i++)
		res = res && insert_copy_of_element_def(httd_p->key_def, NULL, record_def, httd_p->key_element_ids[i]);
	
	// if any of the index element_definitions could not be inserted
	if(!res)
	{
		deinit_hash_table_tuple_definitions(httd_p);
		return 0;
	}

	// end step
	finalize_tuple_def(httd_p->key_def);

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

uint64_t get_hash_table_split_index(uint64_t bucket_count, int64_t* floor_log_2)
{
	(*floor_log_2) = get_floor_log_base_2(bucket_count);
	return bucket_count % (UINT64_C(1) << (*floor_log_2));
}

uint64_t get_hash_value_for_key_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* key)
{
	return hash_tuple(key, httd_p->key_def, NULL, httd_p->hash_func, httd_p->key_element_count);
}

uint64_t get_hash_value_for_record_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple)
{
	return hash_tuple(record_tuple, httd_p->lpltd.record_def, httd_p->key_element_ids, httd_p->hash_func, httd_p->key_element_count);
}

uint64_t get_bucket_index_for_key_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* key, uint64_t bucket_count)
{
	// fetch the hash_value of the key first
	uint64_t hash_value = get_hash_value_for_key_using_hash_table_tuple_definitions(httd_p, key);

	// fetch and cache, the split_index and the floor_log_2 values of the bucket_count
	int64_t fl2;
	uint64_t split_index = get_hash_table_split_index(bucket_count, &fl2);

	// use the below hash_value to bucket_index conversion by default
	uint64_t bucket_index = hash_value % (UINT64_C(1) << fl2);

	// if this bucket was aleady split, then use the new hash_value to bucket_index conversion value
	if(bucket_index < split_index)
	{
		// below if condition is mathematically equivalent to hash_value % (2^(fl2+1))
		if((fl2+1) < 64)
			bucket_index = hash_value % (UINT64_C(1) << (fl2+1));
		else
			bucket_index = hash_value;
	}

	return bucket_index;
}

uint64_t get_bucket_index_for_record_using_hash_table_tuple_definitions(const hash_table_tuple_defs* httd_p, const void* record_tuple, uint64_t bucket_count)
{
	// fetch the hash_value of the key first
	uint64_t hash_value = get_hash_value_for_record_using_hash_table_tuple_definitions(httd_p, record_tuple);

	// fetch and cache, the split_index and the floor_log_2 values of the bucket_count
	int64_t fl2;
	uint64_t split_index = get_hash_table_split_index(bucket_count, &fl2);

	// use the below hash_value to bucket_index conversion by default
	uint64_t bucket_index = hash_value % (UINT64_C(1) << fl2);

	// if this bucket was aleady split, then use the new hash_value to bucket_index conversion value
	if(bucket_index < split_index)
	{
		// below if condition is mathematically equivalent to hash_value % (2^(fl2+1))
		if((fl2+1) != 64)
			bucket_index = hash_value % (UINT64_C(1) << (fl2+1));
		else
			bucket_index = hash_value;
	}

	return bucket_index;
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