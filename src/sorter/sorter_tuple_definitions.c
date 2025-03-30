#include<tupleindexer/sorter/sorter_tuple_definitions.h>

#include<stdlib.h>

int init_sorter_tuple_definitions(sorter_tuple_defs* std_p, const page_access_specs* pas_p, const tuple_def* record_def, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count)
{
	// zero initialize std_p
	(*std_p) = (sorter_tuple_defs){};

	// basic parameter check
	if(key_element_count == 0 || key_element_ids == NULL || record_def == NULL)
		return 0;

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// if positional accessor is invalid catch it here
	if(!are_all_positions_accessible_for_tuple_def(record_def, key_element_ids, key_element_count))
		return 0;

	std_p->key_element_count = key_element_count;

	std_p->key_element_ids = key_element_ids;

	std_p->key_compare_direction = key_compare_direction;

	std_p->record_def = record_def;

	if(!init_linked_page_list_tuple_definitions(&(std_p->lpltd), pas_p, record_def))
	{
		deinit_sorter_tuple_definitions(std_p);
		return 0;
	}

	if(!init_page_table_tuple_definitions(&(std_p->pttd), pas_p))
	{
		deinit_sorter_tuple_definitions(std_p);
		return 0;
	}

	std_p->key_type_infos = malloc(sizeof(data_type_info*) * key_element_count);
	if(std_p->key_type_infos == NULL)
		exit(-1);
	for(uint32_t i = 0; i < key_element_count; i++)
		std_p->key_type_infos[i] = get_type_info_for_element_from_tuple_def(record_def, key_element_ids[i]);

	return 1;
}

int check_if_record_can_be_inserted_for_sorter_tuple_definitions(const sorter_tuple_defs* std_p, const void* record_tuple)
{
	// you cannot insert NULL into the sorter
	if(record_tuple == NULL)
		return 0;

	// if atleast one key element is OUT_OF_BOUNDS then fail
	if(!are_all_positions_accessible_for_tuple(record_tuple, std_p->record_def, std_p->key_element_ids, std_p->key_element_count))
		return 0;

	// if the record can be inserted into a run of the sorter, i.e. in a linked_page_list, then it can be inserted into a sorter
	return check_if_record_can_be_inserted_for_linked_page_list_tuple_definitions(&(std_p->lpltd), record_tuple);
}

void deinit_sorter_tuple_definitions(sorter_tuple_defs* std_p)
{	
	deinit_linked_page_list_tuple_definitions(&(std_p->lpltd));
	deinit_page_table_tuple_definitions(&(std_p->pttd));
	std_p->record_def = NULL;
	std_p->key_element_ids = NULL;
	std_p->key_compare_direction = NULL;
	std_p->key_element_count = 0;
	free(std_p->key_type_infos);
}

void print_sorter_tuple_definitions(const sorter_tuple_defs* std_p)
{
	printf("Sorter tuple defs:\n");

	printf("key_element_count = %"PRIu32"\n", std_p->key_element_count);

	printf("key_element_ids = ");
	if(std_p->key_element_ids)
	{
		printf("{ ");
		for(uint32_t i = 0; i < std_p->key_element_count; i++)
		{
			printf("{ ");
			for(uint32_t j = 0; j < std_p->key_element_ids[i].positions_length; j++)
				printf("%u, ", std_p->key_element_ids[i].positions[j]);
			printf(" }, ");
		}
		printf(" }\n");
	}
	else
		printf("NULL\n");

	printf("key_compare_direction = ");
	if(std_p->key_compare_direction)
	{
		printf("{ ");
		for(uint32_t i = 0; i < std_p->key_element_count; i++)
			printf("%s, ", ((std_p->key_compare_direction[i] == ASC) ? "ASC" : "DESC"));
		printf(" }\n");
	}
	else
		printf("NULL\n");

	printf("record_def = ");
	if(std_p->record_def)
		print_tuple_def(std_p->record_def);
	else
		printf("NULL\n");

	print_linked_page_list_tuple_definitions(&(std_p->lpltd));

	print_page_table_tuple_definitions(&(std_p->pttd));
}