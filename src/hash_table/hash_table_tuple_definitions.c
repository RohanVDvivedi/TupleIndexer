#include<hash_table_tuple_definitions.h>

int init_hash_table_tuple_definitions(hash_table_tuple_defs* httd_p, const page_access_specs* pas_p, const tuple_def* record_def, const uint32_t* key_element_ids, uint32_t key_element_count);

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