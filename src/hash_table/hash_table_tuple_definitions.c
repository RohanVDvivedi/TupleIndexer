#include<hash_table_tuple_definitions.h>

#include<stdlib.h>
#include<string.h>

int init_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p, const page_access_specs* pas_p, const tuple_def* record_def, const uint32_t* key_element_ids, uint32_t key_element_count)
{
	// basic parameter check
	if(key_element_count == 0 || key_element_ids == NULL || record_def == NULL || get_element_def_count_tuple_def(record_def) == 0)
		return 0;

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	httd_p->key_element_count = key_element_count;

	httd_p->key_element_ids = malloc(sizeof(uint32_t) * httd_p->key_element_count);
	if(httd_p->key_element_ids == NULL) // memory allocation failed
		exit(-1);
	memcpy(httd_p->key_element_ids, key_element_ids, sizeof(uint32_t) * httd_p->key_element_count);

	if(!init_linked_page_list_tuple_definitions(&(httd_p->lpltd), pas_p, record_def))
	{
		free(httd_p->key_element_ids);
		return 0;
	}

	if(!init_page_table_tuple_definitions(&(httd_p->pttd), pas_p))
	{
		deinit_linked_page_list_tuple_definitions(&(httd_p->lpltd));
		free(httd_p->key_element_ids);
		return 0;
	}

	return 1;
}

int check_if_record_can_be_inserted_for_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p, const void* record_tuple)
{
	if(record_tuple == NULL)
		return 0;

	return check_if_record_can_be_inserted_for_linked_page_list_tuple_definitions(&(httd_p->lpltd), record_tuple);
}

void deinit_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p);

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