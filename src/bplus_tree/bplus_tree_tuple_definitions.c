#include<bplus_tree_tuple_definitions.h>

#include<persistent_page_functions.h>

#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>

#include<stdlib.h>
#include<string.h>

int init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, const page_access_specs* pas_p, const tuple_def* record_def, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count)
{
	// zero initialize bpttd_p
	(*bpttd_p) = (bplus_tree_tuple_defs){};

	// basic parameter check
	if(key_element_count == 0 || key_element_ids == NULL || record_def == NULL)
		return 0;

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// if positional accessor is invalid catch it here
	if(!are_all_positions_accessible_for_tuple_def(record_def, key_element_ids, key_element_count))
		return 0;

	// initialize page_access_specs fo the bpttd
	bpttd_p->pas_p = pas_p;

	// this can only be called after setting the pas_p attribute of bpttd
	// fail if there is no room after accomodating header on the page
	if((!can_page_header_fit_on_persistent_page(sizeof_BPLUS_TREE_INTERIOR_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size)) || (!can_page_header_fit_on_persistent_page(sizeof_BPLUS_TREE_LEAF_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size)))
		return 0;

	bpttd_p->key_element_count = key_element_count;

	bpttd_p->key_element_ids = key_element_ids;

	bpttd_p->key_compare_direction = key_compare_direction;

	bpttd_p->record_def = record_def;

	// allocate memory for key_def and initialize it
	{
		data_type_info* index_type_info = malloc(sizeof_tuple_data_type_info(key_element_count + 1));
		if(index_type_info == NULL)
			exit(-1);
		initialize_tuple_data_type_info(index_type_info, "temp_index_def", 1, pas_p->page_size, key_element_count + 1);

		for(uint32_t i = 0; i < key_element_count; i++)
		{
			index_type_info->containees[i].field_name[0] = '\0'; // field name here is redundant
			index_type_info->containees[i].type_info = (data_type_info*) get_type_info_for_element_from_tuple_def(record_def, key_element_ids[i]);
		}

		strcpy(index_type_info->containees[key_element_count].field_name, "child_page_id");
		index_type_info->containees[key_element_count].type_info = (data_type_info*) (&(pas_p->page_id_type_info));

		bpttd_p->index_def = malloc(sizeof(tuple_def));
		if(bpttd_p->index_def == NULL)
			exit(-1);
		if(!initialize_tuple_def(bpttd_p->index_def, index_type_info))
		{
			free(bpttd_p->index_def);
			free(index_type_info);
			deinit_bplus_tree_tuple_definitions(bpttd_p);
			return 0;
		}
	}

	// allocate memory for key_def and initialize it
	{
		data_type_info* key_type_info = malloc(sizeof_tuple_data_type_info(key_element_count));
		if(key_type_info == NULL)
			exit(-1);
		initialize_tuple_data_type_info(key_type_info, "temp_key_def", 1, pas_p->page_size, key_element_count);

		for(uint32_t i = 0; i < key_element_count; i++)
		{
			key_type_info->containees[i].field_name[0] = '\0'; // field name here is redundant
			key_type_info->containees[i].type_info = (data_type_info*) get_type_info_for_element_from_tuple_def(record_def, key_element_ids[i]);
		}

		bpttd_p->key_def = malloc(sizeof(tuple_def));
		if(bpttd_p->key_def == NULL)
			exit(-1);
		if(!initialize_tuple_def(bpttd_p->key_def, key_type_info))
		{
			free(bpttd_p->key_def);
			free(key_type_info);
			deinit_bplus_tree_tuple_definitions(bpttd_p);
			return 0;
		}
	}

	/* compute max_record_size for the leaf pages and interior pages of the bplus tree */
	{
		uint32_t min_record_tuple_count = 2;
		uint32_t total_available_space_in_leaf_page = get_space_to_be_allotted_to_all_tuples_on_persistent_page(sizeof_BPLUS_TREE_LEAF_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
		uint32_t total_available_space_in_leaf_page_per_min_tuple_count = total_available_space_in_leaf_page / min_record_tuple_count;
		bpttd_p->max_record_size = total_available_space_in_leaf_page_per_min_tuple_count - get_additional_space_overhead_per_tuple_on_persistent_page(bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
	}

	{
		uint32_t min_index_record_tuple_count = 2;
		uint32_t total_available_space_in_interior_page = get_space_to_be_allotted_to_all_tuples_on_persistent_page(sizeof_BPLUS_TREE_INTERIOR_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));
		uint32_t total_available_space_in_interior_page_per_min_tuple_count = total_available_space_in_interior_page / min_index_record_tuple_count;
		bpttd_p->max_index_record_size = total_available_space_in_interior_page_per_min_tuple_count - get_additional_space_overhead_per_tuple_on_persistent_page(bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));
	}

	if(bpttd_p->max_record_size < get_minimum_tuple_size(bpttd_p->record_def) ||
		bpttd_p->max_index_record_size < get_minimum_tuple_size(bpttd_p->index_def))
	{
		deinit_bplus_tree_tuple_definitions(bpttd_p);
		return 0;
	}

	if(is_fixed_sized_tuple_def(bpttd_p->record_def))
		bpttd_p->max_record_size = bpttd_p->record_def->size_def.size;

	if(is_fixed_sized_tuple_def(bpttd_p->index_def))
		bpttd_p->max_index_record_size = bpttd_p->index_def->size_def.size;

	return 1;
}

int check_if_record_can_be_inserted_for_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple)
{
	// you must not insert a NULL resord in bplus_tree
	if(record_tuple == NULL)
		return 0;

	// if atleast one key element is OUT_OF_BOUNDS then fail
	if(!are_all_positions_accessible_for_tuple(record_tuple, bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count))
		return 0;

	uint32_t record_tuple_size = get_tuple_size(bpttd_p->record_def, record_tuple);

	// if the size of the record tuple is greater than the max_record_size of the bpttd, then it can not be inserted into the bplus_tree with the given bpttd
	if(record_tuple_size > bpttd_p->max_record_size)
		return 0;

	// calcuate the index_record_tuple_size, that this record entry might insert on any of the interior pages
	uint32_t index_record_tuple_size = 0;

	{
		void* temp_index_record_tuple = malloc(bpttd_p->max_index_record_size);
		if(temp_index_record_tuple == NULL)
			exit(-1);
		init_tuple(bpttd_p->index_def, temp_index_record_tuple);
		index_record_tuple_size = get_tuple_size(bpttd_p->index_def, temp_index_record_tuple);

		for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
		{
			int can_set_in_index_tuple = set_element_in_tuple_from_tuple(bpttd_p->index_def, STATIC_POSITION(i), temp_index_record_tuple, bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple, bpttd_p->max_index_record_size - index_record_tuple_size);
			if(!can_set_in_index_tuple)
			{
				free(temp_index_record_tuple);
				return 0;
			}
			index_record_tuple_size = get_tuple_size(bpttd_p->index_def, temp_index_record_tuple);
		}

		// check the size after inserting a child_page_id, 
		int can_set_in_index_tuple = set_element_in_tuple(bpttd_p->index_def, STATIC_POSITION(bpttd_p->key_element_count), temp_index_record_tuple, &((user_value){.uint_value = bpttd_p->pas_p->NULL_PAGE_ID}), bpttd_p->max_index_record_size - index_record_tuple_size);
		if(!can_set_in_index_tuple)
		{
			free(temp_index_record_tuple);
			return 0;
		}
		index_record_tuple_size = get_tuple_size(bpttd_p->index_def, temp_index_record_tuple);

		free(temp_index_record_tuple);
	}

	// if index_record_tuple_size exceeds the max_index_record_size, then this record_tuple can not be inserted in the bplus_tree
	if(index_record_tuple_size > bpttd_p->max_index_record_size)
		return 0;

	return 1;
}

int extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, void* key)
{
	// init the key tuple
	init_tuple(bpttd_p->key_def, key);

	// copy all the elements from record into the key tuple
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->key_def, STATIC_POSITION(i), key, bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple, UINT32_MAX);

	return res;
}

int extract_key_from_index_entry_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* index_entry, void* key)
{
	// init the key tuple
	init_tuple(bpttd_p->key_def, key);

	// copy all the elements from index_entry into the key tuple
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->key_def, STATIC_POSITION(i), key, bpttd_p->index_def, STATIC_POSITION(i), index_entry, UINT32_MAX);

	return res;
}

int build_index_entry_from_record_tuple_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, uint64_t child_page_id, void* index_entry)
{
	// init the index_entry
	init_tuple(bpttd_p->index_def, index_entry);

	// copy all the elements from record to the index_tuple, except for the child_page_id
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->index_def, STATIC_POSITION(i), index_entry, bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple, UINT32_MAX);

	// copy the child_page_id to the last element in the index entry
	if(res == 1)
		res = set_element_in_tuple(bpttd_p->index_def, STATIC_POSITION(bpttd_p->key_element_count), index_entry, &((const user_value){.uint_value = child_page_id}), UINT32_MAX);

	return res;
}

int build_index_entry_from_key_using_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p, const void* key, uint64_t child_page_id, void* index_entry)
{
	// init the index_entry
	init_tuple(bpttd_p->index_def, index_entry);

	// copy all the elements from key to the index_tuple
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->index_def, STATIC_POSITION(i), index_entry, bpttd_p->key_def, STATIC_POSITION(i), key, UINT32_MAX);

	// copy the child_page_id to the last element in the index entry
	if(res == 1)
		set_element_in_tuple(bpttd_p->index_def, STATIC_POSITION(bpttd_p->key_element_count), index_entry, &((user_value){.uint_value = child_page_id}), UINT32_MAX);

	return res;
}

void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p)
{
	if(bpttd_p->index_def)
	{
		if(bpttd_p->index_def->type_info)
			free(bpttd_p->index_def->type_info);
		free(bpttd_p->index_def);
	}
	if(bpttd_p->key_def)
	{
		if(bpttd_p->key_def->type_info)
			free(bpttd_p->key_def->type_info);
		free(bpttd_p->key_def);
	}	

	bpttd_p->pas_p = NULL;
	bpttd_p->key_element_count = 0;
	bpttd_p->key_element_ids = NULL;
	bpttd_p->record_def = NULL;
	bpttd_p->index_def = NULL;
	bpttd_p->key_def = NULL;
	bpttd_p->max_record_size = 0;
	bpttd_p->max_index_record_size = 0;
}

void print_bplus_tree_tuple_definitions(const bplus_tree_tuple_defs* bpttd_p)
{
	printf("Bplus_tree tuple defs:\n");

	if(bpttd_p->pas_p)
		print_page_access_specs(bpttd_p->pas_p);

	printf("key_element_count = %"PRIu32"\n", bpttd_p->key_element_count);

	printf("key_element_ids = ");
	if(bpttd_p->key_element_ids)
	{
		printf("{ ");
		for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
		{
			printf("{ ");
			for(uint32_t j = 0; j < bpttd_p->key_element_ids[i].positions_length; j++)
				printf("%u, ", bpttd_p->key_element_ids[i].positions[j]);
			printf(" }, ");
		}
		printf(" }\n");
	}
	else
		printf("NULL\n");

	printf("key_compare_direction = ");
	if(bpttd_p->key_compare_direction)
	{
		printf("{ ");
		for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
			printf("%s, ", ((bpttd_p->key_compare_direction[i] == ASC) ? "ASC" : "DESC"));
		printf(" }\n");
	}
	else
		printf("NULL\n");

	printf("record_def = ");
	if(bpttd_p->record_def)
		print_tuple_def(bpttd_p->record_def);
	else
		printf("NULL\n");

	printf("index_def = ");
	if(bpttd_p->index_def)
		print_tuple_def(bpttd_p->index_def);
	else
		printf("NULL\n");

	printf("key_def = ");
	if(bpttd_p->key_def)
		print_tuple_def(bpttd_p->key_def);
	else
		printf("NULL\n");

	printf("max_record_size = %"PRIu32"\n", bpttd_p->max_record_size);

	printf("max_index_record_size = %"PRIu32"\n", bpttd_p->max_index_record_size);
}