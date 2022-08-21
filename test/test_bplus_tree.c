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

#define DEFAULT_COMMON_PAGE_HEADER_SIZE 3

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

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	tuple_def* def = get_new_tuple_def("students", 7);

	insert_element_def(def, "index", INT, 4, 0, NULL_USER_VALUE);
	insert_element_def(def, "name", VAR_STRING, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "age", UINT, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "sex", UINT, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "email", VAR_STRING, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "phone", STRING, 14, 0, NULL_USER_VALUE);
	insert_element_def(def, "score", UINT, 1, 0, NULL_USER_VALUE);

	finalize_tuple_def(def, PAGE_SIZE);

	if(is_empty_tuple_def(def))
	{
		printf("ERROR BUILDING TUPLE DEFINITION\n");
		exit(-1);
	}

	print_tuple_def(def);
	printf("\n\n");
	return def;
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

void build_key_tuple_from_record_struct(const bplus_tree_tuple_defs* bpttd_p, void* key_tuple, const record* r)
{
	char record_tuple[PAGE_SIZE];
	build_tuple_from_record_struct(bpttd_p->record_def, record_tuple, r);
	extract_key_from_record_tuple(bpttd_p, record_tuple, key_tuple);
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

typedef struct result result;
struct result
{
	uint32_t operations_succeeded;
	uint32_t records_processed;
};

result insert_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_tree_after_each, int print_tree_on_completion, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// open test data file
	FILE* f = fopen(file_name, "r");

	result res = {};

	uint32_t records_seen = 0;
	while(!feof(f) && (tuples_to_process == 0 || res.records_processed < tuples_to_process))
	{
		// read a record from the file
		record r;
		read_record_from_file(&r, f);

		if(records_seen < skip_first || (records_seen - skip_first) % (skip_every + 1) != 0)
		{
			records_seen++;
			continue;
		}
		else
			records_seen++;

		// print the record we read
		//print_record(&r);

		// construct tuple from this record
		char record_tuple[PAGE_SIZE];
		build_tuple_from_record_struct(bpttd_p->record_def, record_tuple, &r);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

		// insert the record_tuple in the bplus_tree rooted at root_page_id
		res.operations_succeeded += insert_in_bplus_tree(root_page_id, record_tuple, bpttd_p, dam_p);

		// print bplus tree
		if(print_tree_after_each)
		{
			printf("---------------------------------------------------------------------------------------------------------\n");
			print_bplus_tree(root_page_id, 0, bpttd_p, dam_p);
			printf("---------------------------------------------------------------------------------------------------------\n\n");
		}

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print bplus tree
	if(print_tree_on_completion)
		print_bplus_tree(root_page_id, 1, bpttd_p, dam_p);

	// close the file
	fclose(f);

	return res;
}

result delete_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_tree_after_each, int print_tree_on_completion, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// open test data file
	FILE* f = fopen(file_name, "r");

	result res = {};

	uint32_t records_seen = 0;
	while(!feof(f) && (tuples_to_process == 0 || res.records_processed < tuples_to_process))
	{
		// read a record from the file
		record r;
		read_record_from_file(&r, f);

		if(records_seen < skip_first || (records_seen - skip_first) % (skip_every + 1) != 0)
		{
			records_seen++;
			continue;
		}
		else
			records_seen++;

		// print the record we read
		//print_record(&r);

		// construct key tuple from this record
		char key_tuple[PAGE_SIZE];
		build_key_tuple_from_record_struct(bpttd_p, key_tuple, &r);

		// printing built key_tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, key_tuple, bpttd.key_def);
		//printf("Built key_tuple : size(%u)\n\t%s\n\n", get_tuple_size(bpttd.key_def, key_tuple), print_buffer);

		// delete the data corresponding to key_tuple in the bplus_tree rooted at root_page_id
		res.operations_succeeded += delete_from_bplus_tree(root_page_id, key_tuple, bpttd_p, dam_p);

		// print bplus tree
		if(print_tree_after_each)
		{
			printf("---------------------------------------------------------------------------------------------------------\n");
			print_bplus_tree(root_page_id, 0, bpttd_p, dam_p);
			printf("---------------------------------------------------------------------------------------------------------\n\n");
		}

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print bplus tree
	if(print_tree_on_completion)
		print_bplus_tree(root_page_id, 1, bpttd_p, dam_p);

	// close the file
	fclose(f);

	return res;
}

result find_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, uint32_t max_scan_length, uint32_t key_element_count_concerned, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// open test data file
	FILE* f = fopen(file_name, "r");

	result res = {};

	printf("printing the first 4 tuples\n");

	bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, GREATER_THAN, bpttd_p, dam_p);

	const void* tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	uint32_t tuples_to_print = 0;
	while(tuple_to_print != NULL && tuples_to_print < max_scan_length)
	{
		print_tuple(tuple_to_print, bpttd_p->record_def);
		tuples_to_print++;
		next_bplus_tree_iterator(bpi_p);
		tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	}

	delete_bplus_tree_iterator(bpi_p);
	printf("\n");

	printf("printing the last 4 tuples\n");

	bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, LESSER_THAN, bpttd_p, dam_p);

	tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	tuples_to_print = 0;
	while(tuple_to_print != NULL && tuples_to_print < max_scan_length)
	{
		print_tuple(tuple_to_print, bpttd_p->record_def);
		tuples_to_print++;
		prev_bplus_tree_iterator(bpi_p);
		tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	}

	delete_bplus_tree_iterator(bpi_p);
	printf("\n");

	uint32_t records_seen = 0;
	while(!feof(f) && (tuples_to_process == 0 || res.records_processed < tuples_to_process))
	{
		// read a record from the file
		record r;
		read_record_from_file(&r, f);

		if(records_seen < skip_first || (records_seen - skip_first) % (skip_every + 1) != 0)
		{
			records_seen++;
			continue;
		}
		else
			records_seen++;

		// print the record we read
		print_record(&r);

		// construct key tuple from this record
		char key_tuple[PAGE_SIZE];
		build_key_tuple_from_record_struct(bpttd_p, key_tuple, &r);

		find_position find_pos = (LESSER_THAN + (res.records_processed % 4));
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

		bpi_p = find_in_bplus_tree(root_page_id, key_tuple, key_element_count_concerned, find_pos, bpttd_p, dam_p);

		const void* tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
		uint32_t tuples_to_print = 0;
		while(tuple_to_print != NULL && tuples_to_print < max_scan_length)
		{
			print_tuple(tuple_to_print, bpttd_p->record_def);
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
		res.operations_succeeded++;
		res.records_processed++;
	}

	// close the file
	fclose(f);

	return res;
}

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	data_access_methods* dam_p = get_new_in_memory_data_store(PAGE_SIZE, PAGE_ID_WIDTH);

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// construct tuple definitions for bplus_tree
	bplus_tree_tuple_defs bpttd;
	init_bplus_tree_tuple_definitions(&bpttd, DEFAULT_COMMON_PAGE_HEADER_SIZE, record_def, KEY_ELEMENTS_IN_RECORD, KEY_ELEMENTS_COUNT, PAGE_SIZE, PAGE_ID_WIDTH, dam_p->NULL_PAGE_ID);

	// print the generated bplus tree tuple defs
	print_bplus_tree_tuple_definitions(&bpttd);

	// create a bplus tree and get its root
	uint64_t root_page_id = get_new_bplus_tree(&bpttd, dam_p);

	// variable to test insert and delete operations
	result res;

	/* SETUP COMPLETED */



	// insert every 4th tuple from TEST_DATA_FILE
	/* INSERTIONS SARTED */

	res = insert_from_file(root_page_id, TEST_DATA_FILE, 0, 3, 256, 0, 0, &bpttd, dam_p);

	printf("insertions to bplus tree completed (%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* INSERTIONS COMPLETED */



	// again try insert all tuples now 64/256 tuples must fail from TEST_DATA_RANDOM_FILE
	/* INSERTIONS SARTED */

	res = insert_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 0, &bpttd, dam_p);

	printf("insertions to bplus tree completed (%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* INSERTIONS COMPLETED */


	// perfrom find 12 times on tules from TEST_DATA_FILE on all tuples
	/* FIND STARTED */

	res = find_from_file(root_page_id, TEST_DATA_FILE, 3, 5, 12, 6, 1, &bpttd, dam_p);

	printf("finds in bplus tree completed (%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* FIND COMPLETED */


	// delete some random 30 tuples from TEST_DATA_RANDOM_FILE -> 30/30
	/* DELETIONS STARTED */

	res = delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 1, 6, 30, 0, 0, &bpttd, dam_p);

	printf("deletions to bplus tree completed(%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* DELETIONS COMPLETED */



	// perfrom find on remaining tuples
	/* FIND STARTED */

	res = find_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 3, 3, 9, 6, 1, &bpttd, dam_p);

	printf("finds in bplus tree completed (%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* FIND COMPLETED */




	// delete some random 30 tuples from TEST_DATA_RANDOM_FILE -> lesser than 32 success
	/* DELETIONS STARTED */

	res = delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 7, 256, 0, 0, &bpttd, dam_p);

	printf("deletions to bplus tree completed(%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* DELETIONS COMPLETED */




	// perfrom find on remaining tuples
	/* FIND STARTED */

	res = find_from_file(root_page_id, TEST_DATA_FILE, 3, 3, 9, 6, 1, &bpttd, dam_p);

	printf("finds in bplus tree completed (%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* FIND COMPLETED */



	// again insert all from TEST_DATA_RANDOM_FILE -> lesser than 62 failures
	/* INSERTIONS SARTED */

	res = insert_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 0, &bpttd, dam_p);

	printf("insertions to bplus tree completed (%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* INSERTIONS COMPLETED */



	// delete all
	/* DELETIONS STARTED */

	res = delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 0, &bpttd, dam_p);

	printf("deletions to bplus tree completed(%u of %u)\n", res.operations_succeeded, res.records_processed);

	/* DELETIONS COMPLETED */



	/* CLEANUP */

	// destroy bplus tree
	destroy_bplus_tree(root_page_id, &bpttd, dam_p);

	// close the in-memory data store
	close_and_destroy_in_memory_data_store(dam_p);

	// destroy bplus_tree_tuple_definitions
	deinit_bplus_tree_tuple_definitions(&bpttd);

	// delete the record definition
	delete_tuple_def(record_def);

	return 0;
}