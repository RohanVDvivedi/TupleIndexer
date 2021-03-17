#include<stdio.h>
#include<stdlib.h>
#include<alloca.h>

#include<page_list.h>
#include<in_memory_data_store.h>

#include<page_layout.h>
#include<tuple.h>

// comment the below macro to test the SLOTTED_PAGE_LAYOUT
#define TEST_FIXED_ARRAY_PAGE_LAYOUT

#define PAGE_SIZE 512

char temp_page[PAGE_SIZE] = {};

void init_tuple_definition(tuple_def* def)
{
	// initialize tuple definition and insert element definitions
	init_tuple_def(def);

	insert_element_def(def,   UINT, 8);

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
	u8 c0;
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
	#ifndef TEST_FIXED_ARRAY_PAGE_LAYOUT
		copy_element_to_tuple(def, column_no++, tuple, (r->c2));
	#endif
	
	copy_element_to_tuple(def, column_no++, tuple, &(r->c3));
	#ifndef TEST_FIXED_ARRAY_PAGE_LAYOUT
		copy_element_to_tuple(def, column_no++, tuple, (r->c4));
	#endif

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

	// ---------------	DECLARE TEMP variables

	row* r = NULL;

	// ---------------  INITIALIZE DATA STORE

	data_access_methods* dam_p = get_new_in_memory_data_store(PAGE_SIZE, 128);

	// ---------------  CREATE A PAGE LIST

	uint32_t head_page_id = create_new_page_list(dam_p);

	// ---------------  INITIALIZE PAGE CURSOR

	page_cursor* pc_p = alloca(sizeof(page_cursor));
	initialize_cursor(pc_p, WRITER_LOCK, NEXT_PAGE_DIR, head_page_id, def, dam_p);	

	// ---------------  BUILD TESTCASE PAGE LIST

	// ---------------  DEINITIALIZE PAGE CURSOR

	deinitialize_cursor(pc_p);

	// ---------------  DEINITIALIZE DATA ACCESS METHODS

	close_and_destroy_in_memory_data_store(dam_p);

	return 0;
}