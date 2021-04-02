#include<stdio.h>
#include<stdlib.h>

#include<in_memory_data_store.h>
#include<bplus_tree.h>

// comment the below macro to test the SLOTTED_PAGE_LAYOUT
#define TEST_FIXED_ARRAY_PAGE_LAYOUT

#define PAGE_SIZE    128
#define PAGE_COUNT  1024

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
	// initialize record definition
	tuple_def* def = alloca(size_of_tuple_def(16));
	init_tuple_definition(def);

	// initialize tuple definition and insert element definitions for bplus tree
	bplus_tree_tuple_defs* bpttds = get_bplus_tree_tuple_defs_from_record_def(def, 3);

	// initialize data store
	data_access_methods* dam_p = get_new_in_memory_data_store(PAGE_SIZE, PAGE_COUNT);

	// declare temp variables
	char rc[PAGE_SIZE] = {};

	// create a new bplus tree
	uint32_t root_id = create_new_bplus_tree(bpttds, dam_p);

	// ---------- INITIALIZATION COMLETE

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(root_id, bpttds, dam_p);
	printf(" ================== \n\n");

	// test inserts, finds and deletes

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){0, 16, "Rohan", 16, "Dvivedi"}));
	insert_in_bplus_tree(&root_id, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){5, 16, "Samip", 16, "Thakkar"}));
	insert_in_bplus_tree(&root_id, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){10, 16, "Sahil", 16, "Shah"}));
	insert_in_bplus_tree(&root_id, rc, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(root_id, bpttds, dam_p);
	printf(" ================== \n\n");


	// ---------- TESTS COMPLETE

	// cleanup data store
	close_and_destroy_in_memory_data_store(dam_p);

	// clean up bplus tree tuple definition
	del_bplus_tree_tuple_defs(bpttds);
}