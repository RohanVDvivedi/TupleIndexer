#include<bplus_tree_tuple_definitions.h>

#include<page_layout_unaltered.h>

#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>

#include<stdlib.h>
#include<string.h>

int init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, const page_access_specs* pas_p, const tuple_def* record_def, const uint32_t* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count)
{
	// basic parameter check
	if(key_element_count == 0 || key_element_ids == NULL || record_def == NULL || get_element_def_count_tuple_def(record_def) == 0)
		return 0;

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs fo the bpttd
	bpttd_p->pas_p = pas_p;

	// this can only be called after setting the pas_p attribute of bpttd
	// fail if there is no room after accomodating header on the page
	if((!can_page_header_fit_on_page(sizeof_BPLUS_TREE_INTERIOR_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size)) || (!can_page_header_fit_on_page(sizeof_BPLUS_TREE_LEAF_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size)))
		return 0;

	bpttd_p->key_element_count = key_element_count;

	bpttd_p->key_element_ids = malloc(sizeof(uint32_t) * bpttd_p->key_element_count);
	memcpy(bpttd_p->key_element_ids, key_element_ids, sizeof(uint32_t) * bpttd_p->key_element_count);

	bpttd_p->key_compare_direction = malloc(sizeof(compare_direction) * bpttd_p->key_element_count);
	memcpy(bpttd_p->key_compare_direction, key_compare_direction, sizeof(compare_direction) * bpttd_p->key_element_count);

	bpttd_p->record_def = clone_tuple_def(record_def);
	finalize_tuple_def(bpttd_p->record_def);

	// initialize index_def

	// allocate memory for index def and initialize it
	bpttd_p->index_def = get_new_tuple_def("temp_index_def", key_element_count + 1, bpttd_p->pas_p->page_size);

	// result of inserting element_definitions to index def
	int res = 1;

	// initialize element_defs of the index_def
	for(uint32_t i = 0; i < key_element_count && res == 1; i++)
		res = insert_copy_of_element_def(bpttd_p->index_def, NULL, bpttd_p->record_def, bpttd_p->key_element_ids[i]);
	
	// the unsigned int page id that the index entry will point to
	// this element must be a NON NULL, with default_value being bpttd_p->NULL_PAGE_ID
	res = res && insert_element_def(bpttd_p->index_def, "child_page_id", UINT, bpttd_p->pas_p->page_id_width, 1, &((user_value){.uint_value = bpttd_p->pas_p->NULL_PAGE_ID}));

	// if any of the index element_definitions could not be inserted
	if(!res)
	{
		deinit_bplus_tree_tuple_definitions(bpttd_p);
		return 0;
	}

	// end step
	finalize_tuple_def(bpttd_p->index_def);

	// initialize key def

	// allocate memory for key_def and initialize it
	bpttd_p->key_def = get_new_tuple_def("temp_key_def", key_element_count, bpttd_p->pas_p->page_size);

	// result of inserting element_definitions to key_def
	res = 1;

	// initialize element_defs of the key_def
	for(uint32_t i = 0; i < key_element_count && res == 1; i++)
		res = res && insert_copy_of_element_def(bpttd_p->key_def, NULL, bpttd_p->record_def, bpttd_p->key_element_ids[i]);
	
	// if any of the index element_definitions could not be inserted
	if(!res)
	{
		deinit_bplus_tree_tuple_definitions(bpttd_p);
		return 0;
	}

	// end step
	finalize_tuple_def(bpttd_p->key_def);

	/* compute max_record_size for the leaf pages and interior pages of the bplus tree */
	{
		uint32_t min_record_tuple_count = 2;
		uint32_t total_available_space_in_leaf_page = get_space_to_be_allotted_to_all_tuples_on_page(sizeof_BPLUS_TREE_LEAF_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
		uint32_t total_available_space_in_leaf_page_per_min_tuple_count = total_available_space_in_leaf_page / min_record_tuple_count;
		bpttd_p->max_record_size = total_available_space_in_leaf_page_per_min_tuple_count - get_additional_space_overhead_per_tuple_on_page(bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
	}

	{
		uint32_t min_index_record_tuple_count = 2;
		uint32_t total_available_space_in_interior_page = get_space_to_be_allotted_to_all_tuples_on_page(sizeof_BPLUS_TREE_INTERIOR_PAGE_HEADER(bpttd_p), bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));
		uint32_t total_available_space_in_interior_page_per_min_tuple_count = total_available_space_in_interior_page / min_index_record_tuple_count;
		bpttd_p->max_index_record_size = total_available_space_in_interior_page_per_min_tuple_count - get_additional_space_overhead_per_tuple_on_page(bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));
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

int extract_key_from_record_tuple(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, void* key)
{
	// init the key tuple
	init_tuple(bpttd_p->key_def, key);

	// copy all the elements from record into the key tuple
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->key_def, i, key, bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple);

	return res;
}

int extract_key_from_index_entry(const bplus_tree_tuple_defs* bpttd_p, const void* index_entry, void* key)
{
	// init the key tuple
	init_tuple(bpttd_p->key_def, key);

	// copy all the elements from index_entry into the key tuple
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->key_def, i, key, bpttd_p->index_def, i, index_entry);

	return res;
}

int build_index_entry_from_record_tuple(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, uint64_t child_page_id, void* index_entry)
{
	// init the index_entry
	init_tuple(bpttd_p->index_def, index_entry);

	// copy all the elements from record to the index_tuple, except for the child_page_id
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->index_def, i, index_entry, bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple);

	// copy the child_page_id to the last element in the index entry
	if(res == 1)
		res = set_element_in_tuple(bpttd_p->index_def, get_element_def_count_tuple_def(bpttd_p->index_def) - 1, index_entry, &((const user_value){.uint_value = child_page_id}));

	return res;
}

int build_index_entry_from_key(const bplus_tree_tuple_defs* bpttd_p, const void* key, uint64_t child_page_id, void* index_entry)
{
	// init the index_entry
	init_tuple(bpttd_p->index_def, index_entry);

	// copy all the elements from key to the index_tuple
	int res = 1;
	for(uint32_t i = 0; i < bpttd_p->key_element_count && res == 1; i++)
		res = set_element_in_tuple_from_tuple(bpttd_p->index_def, i, index_entry, bpttd_p->key_def, i, key);

	// copy the child_page_id to the last element in the index entry
	if(res == 1)
		set_element_in_tuple(bpttd_p->index_def, get_element_def_count_tuple_def(bpttd_p->index_def) - 1, index_entry, &((user_value){.uint_value = child_page_id}));

	return res;
}

void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p)
{
	if(bpttd_p->record_def)
		delete_tuple_def(bpttd_p->record_def);
	if(bpttd_p->index_def)
		delete_tuple_def(bpttd_p->index_def);
	if(bpttd_p->key_def)
		delete_tuple_def(bpttd_p->key_def);
	if(bpttd_p->key_element_ids)
		free(bpttd_p->key_element_ids);
	if(bpttd_p->key_compare_direction)
		free(bpttd_p->key_compare_direction);

	bpttd_p->pas_p = NULL;
	bpttd_p->key_element_count = 0;
	bpttd_p->key_element_ids = NULL;
	bpttd_p->record_def = NULL;
	bpttd_p->index_def = NULL;
	bpttd_p->key_def = NULL;
	bpttd_p->max_record_size = 0;
	bpttd_p->max_index_record_size = 0;
}

int check_if_record_can_be_inserted_into_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple)
{
	uint32_t record_tuple_size = get_tuple_size(bpttd_p->record_def, record_tuple);

	// if the size of the record tuple is greater than the max_record_size of the bpttd, then it can not be inserted into the bplus_tree with the given bpttd
	if(record_tuple_size > bpttd_p->max_record_size)
		return 0;

	// calcuate the index_record_tuple_size, that this record entry might insert on any of the interior pages
	uint32_t index_record_tuple_size = get_minimum_tuple_size(bpttd_p->index_def);

	for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
	{
		user_value value = get_value_from_element_from_tuple(bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple);
		int can_set_in_index_tuple = can_set_uninitialized_element_in_tuple(bpttd_p->index_def, i, index_record_tuple_size, &value, &index_record_tuple_size);
		if(!can_set_in_index_tuple)
			return 0;
	}

	// check the size after inserting a child_page_id, 
	int can_set_in_index_tuple = can_set_uninitialized_element_in_tuple(bpttd_p->index_def, bpttd_p->key_element_count, index_record_tuple_size, &((user_value){.uint_value = bpttd_p->pas_p->NULL_PAGE_ID}), &index_record_tuple_size);
	if(!can_set_in_index_tuple)
		return 0;

	// if index_record_tuple_size exceeds the max_index_record_size, then this record_tuple can not be inserted in the bplus_tree
	if(index_record_tuple_size > bpttd_p->max_index_record_size)
		return 0;

	return 1;
}

#include<stdio.h>

void print_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p)
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
			printf("%u, ", bpttd_p->key_element_ids[i]);
		printf(" }\n");
	}
	else
		printf("NULL\n");

	printf("key_element_ids = ");
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