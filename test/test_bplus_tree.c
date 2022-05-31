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

#define PAGE_ID_WIDTH 1

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

	// construct an in-memory data store
	data_access_methods* dam_p = get_new_in_memory_data_store(PAGE_SIZE, PAGE_ID_WIDTH);

	// construct tuple definitions for bplus_tree
	bplus_tree_tuple_defs bpttd;
	init_bplus_tree_tuple_definitions(&bpttd, record_def, (uint32_t []){1,4}, 2, PAGE_SIZE, PAGE_ID_WIDTH, dam_p->NULL_PAGE_ID);

	// print the generated bplus tree tuple defs
	print_bplus_tree_tuple_definitions(&bpttd);

	// create a bplus tree and get its root
	uint64_t root_page_id = get_new_bplus_tree(&bpttd, dam_p);

	// stores the count of tuples processed
	uint32_t tuples_processed = 0;

	uint32_t tuples_processed_limit = 256;

	while(!feof(f))
	{
		if(tuples_processed == tuples_processed_limit)
			break;

		// read a row record from the file
		row r;
		read_row_from_file(&r, f);

		// print the row we read
		print_row(&r);

		// construct tuple from this row
		char tuple_csh[PAGE_SIZE];
		build_tuple_from_row_struct(record_def, tuple_csh, &r);

		// insert the tuple_csh in the bplus_tree rooted as root_page_id
		insert_in_bplus_tree(root_page_id, tuple_csh, &bpttd, dam_p);

		// print bplus tree
		printf("---------------------------------------------------------------------------------------------------------\n");
		printf("---------------------------------------------------------------------------------------------------------\n");
		print_bplus_tree(root_page_id, 0, &bpttd, dam_p);
		printf("---------------------------------------------------------------------------------------------------------\n");
		printf("---------------------------------------------------------------------------------------------------------\n");

		// increment the tuples_processed count
		tuples_processed++;
	}

	printf("insertions to bplus tree completed\n");

	// print bplus tree
	print_bplus_tree(root_page_id, 1, &bpttd, dam_p);

	// destroy bplus tree
	destroy_bplus_tree(root_page_id, &bpttd, dam_p);

	// close the in-memory data store
	close_and_destroy_in_memory_data_store(dam_p);

	// destroy bplus_tree_tuple_definitions
	deinit_bplus_tree_tuple_definitions(&bpttd);

	// close the file
	fclose(f);

	return 0;
}