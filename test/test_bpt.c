#include<stdio.h>
#include<stdlib.h>

#include<in_memory_data_store.h>
#include<bplus_tree.h>
#include<read_cursor.h>

// comment the below macro to test the SLOTTED_PAGE_LAYOUT
#define TEST_FIXED_ARRAY_PAGE_LAYOUT

#define PAGE_SIZE    128
//#define PAGE_SIZE    256
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
	bplus_tree_tuple_defs* bpttds = get_bplus_tree_tuple_defs_from_record_def(def, 1);
	print_tuple_def(bpttds->key_def);
	printf("\n\n");
	print_tuple_def(bpttds->index_def);
	printf("\n\n");
	print_tuple_def(bpttds->record_def);
	printf("\n\n");

	// initialize data store
	data_access_methods* dam_p = get_new_in_memory_data_store(PAGE_SIZE, PAGE_COUNT);

	// the bplus tree handle
	bplus_tree_handle bpth;

	// declare temp variables
	char rc[PAGE_SIZE] = {};
	read_cursor btrc;
	int64_t del_id;

	// init a new bplus tree handle
	init_bplus_tree(&bpth, NUL_PAGE_REF);

	// ---------- INITIALIZATION COMLETE

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){0, 16, "Rohan", 16, "Dvivedi"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){5, 16, "Samip", 16, "Thakkar"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){10, 16, "Sahil", 16, "Shah"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){2, 16, "Jyotirmoy", 16, "Pain"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){7, 16, "Shrey", 16, "Bansal"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){12, 16, "Aman", 16, "Garg"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){4, 16, "Harsha", 16, "Grandhi"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){9, 16, "Parthiv", 16, "Kativarapu"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){14, 16, "Anurag", 16, "Anand"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	printf("FIND LOOP\n\n");
	for(int64_t i = 0; i <= 25; i++)
	{
		int found = find_in_bplus_tree(&bpth, &i, &btrc, bpttds, dam_p);
		printf("%ld -> %d -> ", i, found);
		const void* found_record = get_record_from_read_cursor(&btrc, dam_p);
		if(found_record == NULL)
			printf("NULL\n\n");
		else
		{
			char print_str[PAGE_SIZE];
			sprint_tuple(print_str, found_record, bpttds->record_def);
			printf("%s\n\n", print_str);
		}
		close_read_cursor(&btrc, dam_p);
	}
	printf("\n");

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){15, 16, "Vidhan", 16, "Jain"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){17, 16, "Aakash", 16, "Goel"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){20, 16, "Pushpinder", 16, "Patel"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){3, 16, "Sharon", 16, "Teacher"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){11, 16, "Pradeep", 16, "Sir"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){13, 16, "Soumya", 16, "Madam"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){19, 16, "Shubham", 16, "Sir"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){6, 16, "Roopa", 16, "Dvivedi"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){16, 16, "Vipul", 16, "Dvivedi"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){18, 16, "Devashree", 16, "Dvivedi"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	build_tuple_from_row_struct(bpttds->record_def, rc, &((row){21, 16, "Manan", 16, "Joshi"}));
	insert_in_bplus_tree(&bpth, rc, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	del_id = 5; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	del_id = 7; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	del_id = 9; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	del_id = 5; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	del_id = 15; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	del_id = 17; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	del_id = 20; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	del_id = 12; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	del_id = 4; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	del_id = 13; printf("Deleting id : %lu\n\n", del_id);
	delete_in_bplus_tree(&bpth, &del_id, bpttds, dam_p);

	printf(" === BPLUS TREE === \n\n");
	print_bplus_tree(&bpth, bpttds, dam_p);
	printf(" ================== \n\n");

	printf("FIND LOOP\n\n");
	for(int64_t i = 0; i <= 25; i++)
	{
		int found = find_in_bplus_tree(&bpth, &i, &btrc, bpttds, dam_p);
		printf("%ld -> %d -> ", i, found);
		const void* found_record = get_record_from_read_cursor(&btrc, dam_p);
		if(found_record == NULL)
			printf("NULL\n\n");
		else
		{
			char print_str[PAGE_SIZE];
			sprint_tuple(print_str, found_record, bpttds->record_def);
			printf("%s\n\n", print_str);
		}
		close_read_cursor(&btrc, dam_p);
	}
	printf("\n");

	// ---------- TESTS COMPLETE

	// deinitialize the bplus tree handle
	deinit_bplus_tree(&bpth);

	// cleanup data store
	close_and_destroy_in_memory_data_store(dam_p);

	// clean up bplus tree tuple definition
	del_bplus_tree_tuple_defs(bpttds);
}