#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>

#include<tuple.h>
#include<tuple_def.h>

#include<bplus_tree.h>
#include<bplus_tree_tuple_definitions.h>
#include<data_access_methods.h>
#include<in_memory_data_store.h>

#define PAGE_SIZE 256

typedef struct row row;
struct row
{
	uint32_t index;
	char name[64];
	uint8_t age;
	char sex[8]; // Female = 0 or Male = 1
	char email[64];
	char phone[64];
	uint8_t score;
};

void read_from_file(row* r, int fd);

void print_row(row* r);

void init_tuple_definition(tuple_def* def)
{
	// initialize tuple definition and insert element definitions
	int res = init_tuple_def(def, "students");

	res = insert_element_def(def, "index", INT, 4);

	res = insert_element_def(def, "name", VAR_STRING, 1);

	res = insert_element_def(def, "age", UINT, 1);

	res = insert_element_def(def, "sex", UINT, 1);

	res = insert_element_def(def, "email", VAR_STRING, 1);

	res = insert_element_def(def, "phone", STRING, 14);

	res = insert_element_def(def, "score", UINT, 1);

	finalize_tuple_def(def, PAGE_SIZE);

	if(is_empty_tuple_def(def))
	{
		printf("ERROR BUILDING TUPLE DEFINITION\n");
		exit(-1);
	}

	print_tuple_def(def);
	printf("\n\n");
}

void build_tuple_from_row_struct(const tuple_def* def, void* tuple, const row* r)
{
	init_tuple(def, tuple);

	set_element_in_tuple(def, 0, tuple, &(r->index),  4);
	set_element_in_tuple(def, 1, tuple,  (r->name),  -1);
	set_element_in_tuple(def, 2, tuple, &(r->age),    1);
	uint8_t sex = (strcmp(r->sex, "Male") == 0) ? 1 : 0;
	set_element_in_tuple(def, 3, tuple, &(sex),       1);
	set_element_in_tuple(def, 4, tuple,  (r->email), -1);
	set_element_in_tuple(def, 5, tuple,  (r->phone), -1);
	set_element_in_tuple(def, 6, tuple, &(r->score),  1);

	// output print string
	char print_buffer[PAGE_SIZE];

	sprint_tuple(print_buffer, tuple, def);
	printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(def, tuple), print_buffer);
}

void read_row_from_tuple(row* r, const void* tuple)
{
	
}

int main()
{
	return 0;
}