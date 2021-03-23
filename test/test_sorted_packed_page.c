#include<stdio.h>
#include<stdlib.h>
#include<alloca.h>

#include<bplus_tree_util.h>
#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

// comment the below macro to test the SLOTTED_PAGE_LAYOUT
#define TEST_FIXED_ARRAY_PAGE_LAYOUT

#define PAGE_SIZE 512

void init_tuple_definition(tuple_def* def)
{
	// initialize tuple definition and insert element definitions
	init_tuple_def(def);

	insert_element_def(def,   INT, 8);

	insert_element_def(def,   UINT, 2);
	#ifndef TEST_FIXED_ARRAY_PAGE_LAYOUT
		insert_element_def(def, STRING, VARIABLE_SIZED);
	#else
		insert_element_def(def, STRING, 16);
	#endif

	insert_element_def(def,   UINT, 2);
	#ifndef TEST_FIXED_ARRAY_PAGE_LAYOUT
		insert_element_def(def, STRING, VARIABLE_SIZED);
	#else
		insert_element_def(def, STRING, 16);
	#endif

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
	u2 c1;
	char* c2;
	u2 c3;
	char* c4;
};

void build_tuple_from_row_struct(const tuple_def* def, void* tuple, const row* r)
{
	int column_no = 0;

	copy_element_to_tuple(def, column_no++, tuple, &(r->c0));
	copy_element_to_tuple(def, column_no++, tuple, &(r->c1));
	copy_element_to_tuple(def, column_no++, tuple, (r->c2));
	copy_element_to_tuple(def, column_no++, tuple, &(r->c3));
	copy_element_to_tuple(def, column_no++, tuple, (r->c4));

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

	bplus_tree_tuple_defs* bpttds = get_bplus_tree_tuple_defs_from_record_def(def, 3);

	// ---------------	DECLARE TEMP variables

	char page[PAGE_SIZE] = {};
	char tuple_cache[PAGE_SIZE] = {};
	
	row* r = NULL;

	uint16_t inserted_index;
	int inserted;

	// ---------------  INSERTS

	r = &(row){3, 16, "Rohan", 16, "Dvivedi"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){12, 16, "Samip", 16, "Thakkar"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){2, 16, "Rakesh", 16, "Dvivedi"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){6, 16, "Aman", 16, "Patel"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){5, 16, "Sohil", 16, "Shah"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){10, 16, "Sam", 16, "Thakkar"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){9, 16, "Sahil", 16, "Shah"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){4, 16, "Aakash", 16, "Patel"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){15, 16, "Somesh", 16, "Paradkar"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	r = &(row){0, 16, "Hardik", 16, "Pandya"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	inserted = insert_to_sorted_packed_page(page, PAGE_SIZE, bpttds->key_def, bpttds->record_def, tuple_cache, &inserted_index);
	printf("Insert : %d @ [%u]\n\n", inserted, inserted_index);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n\n");

	// ---------------  DELETES

	// ---------------  INSERTS

	// ---------------  DELETES

	// ---------------  DESTROY TUPLE DEFINITION

	del_bplus_tree_tuple_defs(bpttds);

	return 0;
}