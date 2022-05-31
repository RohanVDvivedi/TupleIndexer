#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

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

void read_row_from_file(row* r, FILE* f)
{
	fscanf(f,"%u,%[^,],%hhu,%[^,],%[^,],%[^,],%hhu\n", &(r->index), r->name, &(r->age), r->sex, r->email, r->phone, &(r->score));
}

void print_row(row* r)
{
	printf("row (index = %u, name = %s, age = %u, sex = %s, email = %s, phone = %s, score = %u)\n",
		r->index, r->name, r->age, r->sex, r->email, r->phone, r->score);
}

void init_tuple_definition(tuple_def* def)
{
	// initialize tuple definition and insert element definitions
	init_tuple_def(def, "students");

	insert_element_def(def, "index", INT, 4);
	insert_element_def(def, "name", VAR_STRING, 1);
	insert_element_def(def, "age", UINT, 1);
	insert_element_def(def, "sex", UINT, 1);
	insert_element_def(def, "email", VAR_STRING, 1);
	insert_element_def(def, "phone", STRING, 14);
	insert_element_def(def, "score", UINT, 1);

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

void read_row_from_tuple(row* r, const void* tupl, const tuple_def* tpl_d)
{
	copy_element_from_tuple(tpl_d, 0, tupl, &(r->index));
	copy_element_from_tuple(tpl_d, 1, tupl, r->name);
	copy_element_from_tuple(tpl_d, 2, tupl, &(r->age));
	uint8_t sex = 0;
	strcpy(r->sex, "Female");
	copy_element_from_tuple(tpl_d, 3, tupl, &(sex));
	if(sex)
		strcpy(r->sex, "Male");
	copy_element_from_tuple(tpl_d, 4, tupl, r->email);
	copy_element_from_tuple(tpl_d, 5, tupl, r->phone);
	copy_element_from_tuple(tpl_d, 6, tupl, &(r->score));
}

int main()
{
	// open test data file
	FILE* f = fopen("./testdata.csv","r");

	// allocate record tuple definition and initialize it
	tuple_def* record_def = alloca(size_of_tuple_def(7));
	init_tuple_definition(record_def);

	// stores the count of tuples processed
	uint32_t tuples_processed = 0;

	uint32_t tuples_processed_limit = 20;

	while(!feof(f))
	{
		if(tuples_processed == tuples_processed_limit)
			break;
	
		row r;

		// read a row record from the file
		read_row_from_file(&r, f);

		// print the row we read
		print_row(&r);

		char tuple_csh[PAGE_SIZE];

		// construct tuple from this row
		build_tuple_from_row_struct(record_def, tuple_csh, &r);

		tuples_processed++;
	}

	fclose(f);
	return 0;
}