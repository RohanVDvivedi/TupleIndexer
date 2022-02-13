#include<sorted_packed_page_util.h>

#include<stdio.h>
#include<stdlib.h>
#include<string.h>

#include<alloca.h>

#include<tuple.h>
#include<page_layout.h>

#define PAGE_SIZE 1024

char page[PAGE_SIZE] = {};

char temp_page[PAGE_SIZE] = {};

void init_tuple_definition(tuple_def* def)
{
	// initialize tuple definition and insert element definitions
	int res = init_tuple_def(def, "my_table");

	res = insert_element_def(def, "key_1", VAR_STRING, 1);

	res = insert_element_def(def, "key_2", INT, 8);

	res = insert_element_def(def, "val", VAR_STRING, 1);

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
	char* key1;
	int64_t key2;
	char* val;
};

void build_tuple_from_row_struct(const tuple_def* def, void* tuple, const row* r)
{
	int column_no = 0;

	copy_element_to_tuple(def, column_no++, tuple, (r->key1), -1);
	copy_element_to_tuple(def, column_no++, tuple, &(r->key2), -1);
	copy_element_to_tuple(def, column_no++, tuple, (r->val), -1);

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

	// to build intermediate tuples (only 1 at a time)
	char tuple_cache[PAGE_SIZE];
	// and
	row* r = NULL;
	// and the result from operation
	int res;

	// ---------------  INITIALIZE PAGE

	if(!init_page(page, PAGE_SIZE, 0, def))
	{
		printf("ERROR INITIALIZING THE PAGE\n");
		exit(-1);
	}

	// ---------------	INSERT

	r = &(row){1, "Rohan", "Son"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ---------------	INSERT

	r = &(row){0, "Devashree", "Daughter"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");
	
	return 0;
}