#include<bplus_tree_tuple_definitions.h>

#include<page_layout.h>
#include<tuple.h>

#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>

#include<stdlib.h>
#include<string.h>

int init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, uint32_t default_common_header_size, const tuple_def* record_def, const uint32_t* key_element_ids, uint32_t key_element_count, uint32_t page_size, uint8_t page_id_width, uint64_t NULL_PAGE_ID)
{
	// basic parameter check
	if(key_element_count == 0 || key_element_ids == NULL || record_def == NULL || record_def->element_count == 0)
		return 0;

	// bytes required to store page id
	if(page_id_width == 0 || page_id_width > 8)
		return 0;

	// NULL_PAGE_ID must fit in page_id_width number of bytes
	if(page_id_width < 8 && NULL_PAGE_ID >= ( ((uint64_t)(1)) << (page_id_width * 8) ) )
		return 0;

	// initialize struct attributes
	bpttd_p->NULL_PAGE_ID = NULL_PAGE_ID;
	bpttd_p->page_id_width = page_id_width;
	bpttd_p->page_size = page_size;
	bpttd_p->key_element_count = key_element_count;

	bpttd_p->default_common_header_size = default_common_header_size;

	if(sizeof_INTERIOR_PAGE_HEADER(bpttd_p) >= page_size || sizeof_LEAF_PAGE_HEADER(bpttd_p) >= page_size)
		return 0;

	bpttd_p->key_element_ids = malloc(sizeof(uint32_t) * bpttd_p->key_element_count);
	memcpy(bpttd_p->key_element_ids, key_element_ids, sizeof(uint32_t) * bpttd_p->key_element_count);

	bpttd_p->record_def = clone_tuple_def(record_def);
	finalize_tuple_def(bpttd_p->record_def, page_size);

	// initialize index_def

	// allocate memory for index def and initialize it
	bpttd_p->index_def = get_new_tuple_def("temp_index_def", key_element_count + 1);

	// result of inserting element_definitions to index def
	int res = 1;

	// initialize element_defs of the index_def
	for(uint32_t i = 0; i < key_element_count && res == 1; i++)
		res = insert_copy_of_element_def(bpttd_p->index_def, NULL, bpttd_p->record_def, bpttd_p->key_element_ids[i]);
	
	// the unsigned int page id that the index entry will point to
	// this element must be a NON NULL, with default_value being bpttd_p->NULL_PAGE_ID
	res = res && insert_element_def(bpttd_p->index_def, "child_page_id", UINT, page_id_width, 1, &((user_value){.uint_value = bpttd_p->NULL_PAGE_ID}));

	// if any of the index element_definitions could not be inserted
	if(!res)
	{
		deinit_bplus_tree_tuple_definitions(bpttd_p);
		return 0;
	}

	// end step
	finalize_tuple_def(bpttd_p->index_def, page_size);

	// initialize key def

	// allocate memory for key_def and initialize it
	bpttd_p->key_def = get_new_tuple_def("temp_key_def", key_element_count);

	// result of inserting element_definitions to key_def
	res = 1;

	// initialize element_defs of the key_def
	for(uint32_t i = 0; i < key_element_count && res == 1; i++)
		res = insert_copy_of_element_def(bpttd_p->key_def, NULL, bpttd_p->record_def, bpttd_p->key_element_ids[i]);
	
	// if any of the index element_definitions could not be inserted
	if(!res)
	{
		deinit_bplus_tree_tuple_definitions(bpttd_p);
		return 0;
	}

	// end step
	finalize_tuple_def(bpttd_p->key_def, page_size);

	if(get_maximum_insertable_record_size(bpttd_p) < get_minimum_tuple_size(bpttd_p->record_def))
	{
		deinit_bplus_tree_tuple_definitions(bpttd_p);
		return 0;
	}

	return 1;
}

void extract_key_from_record_tuple(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, void* key)
{
	// init the key tuple
	init_tuple(bpttd_p->key_def, key);

	// copy all the elements from record into the key tuple
	for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
		set_element_in_tuple_from_tuple(bpttd_p->key_def, i, key, bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple);
}

void extract_key_from_index_entry(const bplus_tree_tuple_defs* bpttd_p, const void* index_entry, void* key)
{
	// init the key tuple
	init_tuple(bpttd_p->key_def, key);

	// copy all the elements from index_entry into the key tuple
	for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
		set_element_in_tuple_from_tuple(bpttd_p->key_def, i, key, bpttd_p->index_def, i, index_entry);
}

void build_index_entry_from_record_tuple(const bplus_tree_tuple_defs* bpttd_p, const void* record_tuple, uint64_t child_page_id, void* index_entry)
{
	// init the index_entry
	init_tuple(bpttd_p->index_def, index_entry);

	// copy all the elements from record to the index_tuple, except for the child_page_id
	for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
		set_element_in_tuple_from_tuple(bpttd_p->index_def, i, index_entry, bpttd_p->record_def, bpttd_p->key_element_ids[i], record_tuple);

	// copy the child_page_id to the last element in the index entry
	set_element_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_entry, &((const user_value){.uint_value = child_page_id}));
}

void build_index_entry_from_key(const bplus_tree_tuple_defs* bpttd_p, const void* key, uint64_t child_page_id, void* index_entry)
{
	// init the index_entry
	init_tuple(bpttd_p->index_def, index_entry);

	// copy all the elements from key to the index_tuple
	for(uint32_t i = 0; i < bpttd_p->key_element_count; i++)
		set_element_in_tuple_from_tuple(bpttd_p->index_def, i, index_entry, bpttd_p->key_def, i, key);

	// copy the child_page_id to the last element in the index entry
	set_element_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_entry, &((user_value){.uint_value = child_page_id}));
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

	bpttd_p->NULL_PAGE_ID = 0;
	bpttd_p->page_id_width = 0;
	bpttd_p->page_size = 0;
	bpttd_p->key_element_count = 0;
	bpttd_p->key_element_ids = NULL;
	bpttd_p->record_def = NULL;
	bpttd_p->index_def = NULL;
	bpttd_p->key_def = NULL;
}

uint32_t get_maximum_insertable_record_size(const bplus_tree_tuple_defs* bpttd_p)
{
	uint32_t min_record_tuple_count = 2;
	uint32_t total_available_space_in_leaf_page = get_space_to_be_allotted_to_all_tuples(sizeof_LEAF_PAGE_HEADER(bpttd_p), bpttd_p->page_size, bpttd_p->record_def);
	uint32_t total_available_space_in_leaf_page_per_min_tuple_count = total_available_space_in_leaf_page / min_record_tuple_count;
	uint32_t max_record_size_per_leaf_page = total_available_space_in_leaf_page_per_min_tuple_count - get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->record_def);

	uint32_t min_index_record_tuple_count = 2;
	uint32_t total_available_space_in_interior_page = get_space_to_be_allotted_to_all_tuples(sizeof_INTERIOR_PAGE_HEADER(bpttd_p), bpttd_p->page_size, bpttd_p->index_def);
	uint32_t total_available_space_in_interior_page_per_min_tuple_count = total_available_space_in_interior_page / min_index_record_tuple_count;
	uint32_t max_key_size_per_interior_page = total_available_space_in_interior_page_per_min_tuple_count - get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->index_def) - bpttd_p->page_id_width - 1; 
	// this 1 at the end is important believe me, it represents a spill over bit from storing the is_null bit for the child page pointer

	#define min(a,b) (((a)<(b))?(a):(b))
	return  min(max_record_size_per_leaf_page, max_key_size_per_interior_page);
}

#include<stdio.h>

void print_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p)
{
	printf("Bplus_tree tuple defs:\n");

	printf("page_id_width = %"PRIu8"\n", bpttd_p->page_id_width);

	printf("page_size = %"PRIu32"\n", bpttd_p->page_size);

	printf("NULL_PAGE_ID = %"PRIu64"\n", bpttd_p->NULL_PAGE_ID);

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
}