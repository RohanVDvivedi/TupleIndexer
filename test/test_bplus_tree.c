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

// un comment based on the keys that you want to test with
#define KEY_NAME_EMAIL
//#define KEY_INDEX_PHONE
//#define KEY_PHONE_SCORE
//#define KEY_EMAIL_AGE_SEX

#if defined KEY_NAME_EMAIL
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){1,4}
#elif defined KEY_INDEX_PHONE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){0,5}
#elif defined KEY_PHONE_SCORE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){5,6}
#elif defined KEY_EMAIL_AGE_SEX
	#define KEY_ELEMENTS_COUNT			3
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){4,2,3}
#endif

#define PAGE_SIZE 256

#define PAGE_ID_WIDTH 1

typedef struct record record;
struct record
{
	uint32_t index; // 0
	char name[64];  // 1
	uint8_t age;    // 2
	char sex[8];    // 3 -> Female = 0 or Male = 1
	char email[64]; // 4
	char phone[64]; // 5
	uint8_t score;  // 6
};

void read_record_from_file(record* r, FILE* f)
{
	fscanf(f,"%u,%[^,],%hhu,%[^,],%[^,],%[^,],%hhu\n", &(r->index), r->name, &(r->age), r->sex, r->email, r->phone, &(r->score));
}

void print_record(record* r)
{
	printf("record (index = %u, name = %s, age = %u, sex = %s, email = %s, phone = %s, score = %u)\n",
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

void build_tuple_from_record_struct(const tuple_def* def, void* tuple, const record* r)
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

void build_key_tuple_from_record_struct(const tuple_def* key_def, void* key_tuple, const record* r)
{
	init_tuple(key_def, key_tuple);

	#if defined KEY_NAME_EMAIL
		set_element_in_tuple(key_def, 0, key_tuple,  (r->name),  -1);
		set_element_in_tuple(key_def, 1, key_tuple,  (r->email), -1);
	#elif defined KEY_INDEX_PHONE
		set_element_in_tuple(key_def, 0, key_tuple, &(r->index),  4);
		set_element_in_tuple(key_def, 1, key_tuple,  (r->phone), -1);
	#elif defined KEY_PHONE_SCORE
		set_element_in_tuple(key_def, 0, key_tuple,  (r->phone), -1);
		set_element_in_tuple(key_def, 1, key_tuple, &(r->score),  1);
	#elif defined KEY_EMAIL_AGE_SEX
		set_element_in_tuple(key_def, 0, key_tuple,  (r->email), -1);
		set_element_in_tuple(key_def, 1, key_tuple, &(r->age),    1);
		set_element_in_tuple(key_def, 2, key_tuple, &(sex),       1);
	#endif

	// output print string
	char print_buffer[PAGE_SIZE];

	sprint_tuple(print_buffer, key_tuple, key_def);
	printf("Built key_tuple : size(%u)\n\t%s\n\n", get_tuple_size(key_def, key_tuple), print_buffer);
}

void read_record_from_tuple(record* r, const void* tupl, const tuple_def* tpl_d)
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
	/* SETUP STARTED */

	// allocate record tuple definition and initialize it
	tuple_def* record_def = alloca(size_of_tuple_def(7));
	init_tuple_definition(record_def);

	// construct an in-memory data store
	data_access_methods* dam_p = get_new_in_memory_data_store(PAGE_SIZE, PAGE_ID_WIDTH);

	// construct tuple definitions for bplus_tree
	bplus_tree_tuple_defs bpttd;
	init_bplus_tree_tuple_definitions(&bpttd, record_def, KEY_ELEMENTS_IN_RECORD, KEY_ELEMENTS_COUNT, PAGE_SIZE, PAGE_ID_WIDTH, dam_p->NULL_PAGE_ID);

	// print the generated bplus tree tuple defs
	print_bplus_tree_tuple_definitions(&bpttd);

	// create a bplus tree and get its root
	uint64_t root_page_id = get_new_bplus_tree(&bpttd, dam_p);

	/* SETUP COMPLETED */



	/* INSERTIONS SARTED */

	// open test data file
	FILE* f = fopen("./testdata.csv","r");

	// stores the count of tuples processed
	uint32_t tuples_processed = 0;

	uint32_t tuples_processed_limit = 256;

	while(!feof(f))
	{
		if(tuples_processed == tuples_processed_limit)
			break;

		// read a record from the file
		record r;
		read_record_from_file(&r, f);

		// print the record we read
		print_record(&r);

		// construct tuple from this record
		char record_tuple[PAGE_SIZE];
		build_tuple_from_record_struct(record_def, record_tuple, &r);

		// insert the record_tuple in the bplus_tree rooted at root_page_id
		insert_in_bplus_tree(root_page_id, record_tuple, &bpttd, dam_p);

		// print bplus tree
		//printf("---------------------------------------------------------------------------------------------------------\n");
		//printf("---------------------------------------------------------------------------------------------------------\n");
		//print_bplus_tree(root_page_id, 0, &bpttd, dam_p);
		//printf("---------------------------------------------------------------------------------------------------------\n");
		//printf("---------------------------------------------------------------------------------------------------------\n");

		// increment the tuples_processed count
		tuples_processed++;
	}

	// print bplus tree
	print_bplus_tree(root_page_id, 0, &bpttd, dam_p);

	// close the file
	fclose(f);

	printf("insertions to bplus tree completed\n");

	/* INSERTIONS COMPLETED */



	/* DELETIONS STARTED */

	// open test data file
	f = fopen("./testdata_random.csv","r");

	// stores the count of tuples processed
	tuples_processed = 0;

	tuples_processed_limit = 256;

	while(!feof(f))
	{
		if(tuples_processed == tuples_processed_limit)
			break;

		// read a record from the file
		record r;
		read_record_from_file(&r, f);

		// print the record we read
		print_record(&r);

		// construct key tuple from this record
		char key_tuple[PAGE_SIZE];
		build_key_tuple_from_record_struct(bpttd.key_def, key_tuple, &r);

		// delete the data corresponding to key_tuple in the bplus_tree rooted at root_page_id
		delete_from_bplus_tree(root_page_id, key_tuple, &bpttd, dam_p);

		// print bplus tree
		printf("---------------------------------------------------------------------------------------------------------\n");
		printf("---------------------------------------------------------------------------------------------------------\n");
		print_bplus_tree(root_page_id, 0, &bpttd, dam_p);
		printf("---------------------------------------------------------------------------------------------------------\n");
		printf("---------------------------------------------------------------------------------------------------------\n");

		// increment the tuples_processed count
		tuples_processed++;
	}

	// print bplus tree
	//print_bplus_tree(root_page_id, 0, &bpttd, dam_p);

	// close the file
	fclose(f);

	printf("deletions to bplus tree completed\n");

	/* DELETIONS COMPLETED */



	/* CLEANUP */

	// destroy bplus tree
	destroy_bplus_tree(root_page_id, &bpttd, dam_p);

	// close the in-memory data store
	close_and_destroy_in_memory_data_store(dam_p);

	// destroy bplus_tree_tuple_definitions
	deinit_bplus_tree_tuple_definitions(&bpttd);

	return 0;
}