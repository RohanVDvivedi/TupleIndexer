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

#define TEST_DATA_FILE         "./testdata.csv"
#define TEST_DATA_RANDOM_FILE  "./testdata_random.csv"

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

#define PAGE_ID_WIDTH 3

typedef struct record record;
struct record
{
	int32_t index;  // 0
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

	insert_element_def(def, "index", INT, 4, 0, &NULL_USER_VALUE);
	insert_element_def(def, "name", VAR_STRING, 1, 0, &NULL_USER_VALUE);
	insert_element_def(def, "age", UINT, 1, 0, &NULL_USER_VALUE);
	insert_element_def(def, "sex", UINT, 1, 0, &NULL_USER_VALUE);
	insert_element_def(def, "email", VAR_STRING, 1, 0, &NULL_USER_VALUE);
	insert_element_def(def, "phone", STRING, 14, 0, &NULL_USER_VALUE);
	insert_element_def(def, "score", UINT, 1, 0, &NULL_USER_VALUE);

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

	set_element_in_tuple(def, 0, tuple, &((user_value){.int_value = r->index}));
	set_element_in_tuple(def, 1, tuple, &((user_value){.data = r->name, .data_size = strlen(r->name)}));
	set_element_in_tuple(def, 2, tuple, &((user_value){.uint_value = r->age}));
	set_element_in_tuple(def, 3, tuple, &((user_value){.uint_value = ((strcmp(r->sex, "Male") == 0) ? 1 : 0)}));
	set_element_in_tuple(def, 4, tuple, &((user_value){.data = r->email, .data_size = strlen(r->email)}));
	set_element_in_tuple(def, 5, tuple, &((user_value){.data = r->phone, .data_size = strlen(r->phone)}));
	set_element_in_tuple(def, 6, tuple, &((user_value){.uint_value = r->score}));
}

void build_key_tuple_from_record_struct(const tuple_def* key_def, void* key_tuple, const record* r)
{
	init_tuple(key_def, key_tuple);

	#if defined KEY_NAME_EMAIL
		set_element_in_tuple(key_def, 0, key_tuple, &((user_value){.data = r->name, .data_size = strlen(r->name)}));
		set_element_in_tuple(key_def, 1, key_tuple, &((user_value){.data = r->email, .data_size = strlen(r->email)}));
	#elif defined KEY_INDEX_PHONE
		set_element_in_tuple(key_def, 0, key_tuple, &((user_value){.int_value = r->index}));
		set_element_in_tuple(key_def, 1, key_tuple, &((user_value){.data = r->phone, .data_size = strlen(r->phone)}));
	#elif defined KEY_PHONE_SCORE
		set_element_in_tuple(key_def, 0, key_tuple, &((user_value){.data = r->phone, .data_size = strlen(r->phone)}));
		set_element_in_tuple(key_def, 1, key_tuple, &((user_value){.uint_value = r->score}));
	#elif defined KEY_EMAIL_AGE_SEX
		set_element_in_tuple(key_def, 0, key_tuple, &((user_value){.data = r->email, .data_size = strlen(r->email)}));
		set_element_in_tuple(key_def, 1, key_tuple, &((user_value){.uint_value = r->age}));
		set_element_in_tuple(key_def, 2, key_tuple, &((user_value){.uint_value = ((strcmp(r->sex, "Male") == 0) ? 1 : 0)}));
	#endif
}

void read_record_from_tuple(record* r, const void* tupl, const tuple_def* tpl_d)
{
	r->index = get_value_from_element_from_tuple(tpl_d, 0, tupl).int_value;
	user_value name_data = get_value_from_element_from_tuple(tpl_d, 1, tupl);
	strncpy(r->name, name_data.data, name_data.data_size);
	r->age = get_value_from_element_from_tuple(tpl_d, 2, tupl).uint_value;
	uint8_t sex = 0;
	strcpy(r->sex, "Female");
	sex = get_value_from_element_from_tuple(tpl_d, 3, tupl).uint_value;
	if(sex)
		strcpy(r->sex, "Male");
	user_value email_data = get_value_from_element_from_tuple(tpl_d, 4, tupl);
	strncpy(r->email, email_data.data, email_data.data_size);
	user_value phone_data = get_value_from_element_from_tuple(tpl_d, 5, tupl);
	strncpy(r->phone, phone_data.data, phone_data.data_size);
	r->score = get_value_from_element_from_tuple(tpl_d, 6, tupl).uint_value;
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
	FILE* f = fopen(TEST_DATA_FILE, "r");

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
		//print_record(&r);

		// construct tuple from this record
		char record_tuple[PAGE_SIZE];
		build_tuple_from_record_struct(record_def, record_tuple, &r);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

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
	print_bplus_tree(root_page_id, 1, &bpttd, dam_p);

	// close the file
	fclose(f);

	printf("insertions to bplus tree completed\n");

	/* INSERTIONS COMPLETED */





	/* FIND STARTED */

	// open test data file
	f = fopen(TEST_DATA_FILE, "r");

	// stores the count of tuples processed
	tuples_processed = 0;

	tuples_processed_limit = 12;

	uint32_t found_tuples_to_print = 4;

	printf("printing the first 4 tuples\n");

	bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, GREATER_THAN, &bpttd, dam_p);

	const void* tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	uint32_t tuples_to_print = 0;
	while(tuple_to_print != NULL && tuples_to_print < found_tuples_to_print)
	{
		print_tuple(tuple_to_print, bpttd.record_def);
		tuples_to_print++;
		next_bplus_tree_iterator(bpi_p);
		tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	}

	delete_bplus_tree_iterator(bpi_p);
	printf("\n");

	printf("printing the last 4 tuples\n");

	bpi_p = find_in_bplus_tree(root_page_id, NULL, LESSER_THAN, &bpttd, dam_p);

	tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	tuples_to_print = 0;
	while(tuple_to_print != NULL && tuples_to_print < found_tuples_to_print)
	{
		print_tuple(tuple_to_print, bpttd.record_def);
		tuples_to_print++;
		prev_bplus_tree_iterator(bpi_p);
		tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	}

	delete_bplus_tree_iterator(bpi_p);
	printf("\n");

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

		find_position find_pos = (LESSER_THAN + (tuples_processed % 4));
		switch(find_pos)
		{
			case LESSER_THAN :
			{
				printf("LESSER_THAN\n\n");
				break;
			}
			case LESSER_THAN_EQUALS :
			{
				printf("LESSER_THAN_EQUALS\n\n");
				break;
			}
			case GREATER_THAN_EQUALS :
			{
				printf("GREATER_THAN_EQUALS\n\n");
				break;
			}
			case GREATER_THAN :
			{
				printf("GREATER_THAN\n\n");
				break;
			}
		}

		bpi_p = find_in_bplus_tree(root_page_id, key_tuple, find_pos, &bpttd, dam_p);

		const void* tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
		uint32_t tuples_to_print = 0;
		while(tuple_to_print != NULL && tuples_to_print < found_tuples_to_print)
		{
			print_tuple(tuple_to_print, bpttd.record_def);
			tuples_to_print++;
			switch(find_pos)
			{
				case LESSER_THAN :
				case LESSER_THAN_EQUALS :
				{
					prev_bplus_tree_iterator(bpi_p);
					break;
				}
				case GREATER_THAN_EQUALS :
				case GREATER_THAN :
				{
					next_bplus_tree_iterator(bpi_p);
					break;
				}
			}
			tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
		}

		delete_bplus_tree_iterator(bpi_p);
		printf("\n");
		printf("----------------\n\n");

		// increment the tuples_processed count
		tuples_processed++;
	}

	// close the file
	fclose(f);

	printf("finds in bplus tree completed\n");

	/* FIND COMPLETED */





	/* DELETIONS STARTED */

	// open test data file
	f = fopen(TEST_DATA_RANDOM_FILE, "r");

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
		//print_record(&r);

		// construct key tuple from this record
		char key_tuple[PAGE_SIZE];
		build_key_tuple_from_record_struct(bpttd.key_def, key_tuple, &r);

		// printing built key_tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, key_tuple, bpttd.key_def);
		//printf("Built key_tuple : size(%u)\n\t%s\n\n", get_tuple_size(bpttd.key_def, key_tuple), print_buffer);

		// delete the data corresponding to key_tuple in the bplus_tree rooted at root_page_id
		delete_from_bplus_tree(root_page_id, key_tuple, &bpttd, dam_p);

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
	print_bplus_tree(root_page_id, 1, &bpttd, dam_p);

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