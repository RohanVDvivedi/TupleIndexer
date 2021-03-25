#include<stdio.h>
#include<stdlib.h>
#include<alloca.h>

#include<bplus_tree_util.h>
#include<bplus_tree_interior_page_util.h>
#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

// comment the below macro to test the SLOTTED_PAGE_LAYOUT
#define TEST_FIXED_ARRAY_PAGE_LAYOUT

#define PAGE_SIZE 1024

void init_tuple_definition(tuple_def* def)
{
	// initialize tuple definition and insert element definitions
	init_tuple_def(def);

	insert_element_def(def,    INT, 8);
	insert_element_def(def,   UINT, 4);

	finalize_tuple_def(def);

	if(is_empty_tuple_def(def))
	{
		printf("ERROR BUILDING TUPLE DEFINITION\n");
		exit(-1);
	}

	print_tuple_def(def);
	printf("\n\n");
}

// a row like struct for ease in building test tuples
typedef struct row row;
struct row
{
	i8 c0;
	u4 c1;
};

void build_tuple_from_row_struct(const tuple_def* def, void* tuple, const row* r)
{
	int column_no = 0;

	copy_element_to_tuple(def, column_no++, tuple, &(r->c0));
	copy_element_to_tuple(def, column_no++, tuple, &(r->c1));

	// output print string
	char print_buffer[PAGE_SIZE];

	sprint_tuple(print_buffer, tuple, def);
	printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(def, tuple), print_buffer);
}

int main()
{
	// allocate size of tuple definition
	tuple_def* def = alloca(size_of_tuple_def(16));

	// initialize tuple definition and insert element definitions
	init_tuple_definition(def);

	bplus_tree_tuple_defs* bpttds = get_bplus_tree_tuple_defs_from_record_def(def, 1);

	printf("KEY DEF:\n");
	print_tuple_def(bpttds->key_def);
	printf("\n\n");
	
	printf("INDEX DEF :\n");
	print_tuple_def(bpttds->index_def);
	printf("\n\n");

	printf("RECORD DEF :\n");
	print_tuple_def(bpttds->record_def);
	printf("\n\n");

	// ---------------	DECLARE TEMP variables

	char page[PAGE_SIZE] = {};
	char tuple_cache[PAGE_SIZE] = {};
	
	row* r = NULL;

	uint16_t inserted_index;
	int inserted;

	// ---------------  INIT PAGE

	init_page(page, PAGE_SIZE, INTERIOR_PAGE_TYPE, 1, bpttds->index_def);

	// ---------------  INSERTS

	r = &(row){0, 1};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){5, 2};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){10, 3};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){15, 4};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){20, 5};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){25, 6};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){30, 7};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){35, 8};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){40, 9};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){45, 10};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){50, 11};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){55, 12};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	// ---------------  MODS TO REFERENCE PAGE IDS

	for(int32_t i = -1; i < get_tuple_count(page); i++)
		set_index_page_id_in_interior_page(page, PAGE_SIZE, i, bpttds, (1 << (i+1)));

	for(int32_t i = -1; i < get_tuple_count(page); i++)
		printf("%d => %u\n", i, get_index_page_id_from_interior_page(page, PAGE_SIZE, i, bpttds));

	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	// ---------------  SEARCHES

	// ---------------  DESTROY TUPLE DEFINITION

	del_bplus_tree_tuple_defs(bpttds);

	return 0;
}