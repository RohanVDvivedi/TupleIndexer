#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuple.h>
#include<tuple_def.h>

#include<sorter.h>
#include<linked_page_list.h>

#include<unWALed_in_memory_data_store.h>
#include<unWALed_page_modification_methods.h>

// uncomment based on the keys that you want to test with
#define KEY_NAME_EMAIL
//#define KEY_INDEX_PHONE
//#define KEY_PHONE_SCORE
//#define KEY_EMAIL_AGE_SEX
//#define KEY_SEX_EMAIL
//#define KEY_SCORE_INDEX
//#define KEY_SCORE_NAME

#define TEST_DATA_FILE         "./testdata.csv"
#define TEST_DATA_RANDOM_FILE  "./testdata_random.csv"

#if defined KEY_NAME_EMAIL
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){1,4}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_INDEX_PHONE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){0,5}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_PHONE_SCORE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){5,6}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_EMAIL_AGE_SEX
	#define KEY_ELEMENTS_COUNT			3
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){4,2,3}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC,ASC}
#elif defined KEY_SEX_EMAIL
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){3,4}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_SCORE_INDEX
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){6,0}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_SCORE_NAME
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(uint32_t []){6,1}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#endif

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256
#define SYSTEM_HEADER_SIZE   3

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

typedef struct record record;
struct record
{
	int32_t index;   // 0
	char name[64];   // 1
	uint8_t age;     // 2
	char sex[8];     // 3 -> Female = 0 or Male = 1
	char email[64];  // 4
	char phone[64];  // 5
	uint8_t score;   // 6
	char update[64]; // 7
};

void read_record_from_file(record* r, FILE* f)
{
	memset(r->update, 0, 64);
	fscanf(f,"%u,%[^,],%hhu,%[^,],%[^,],%[^,],%hhu\n", &(r->index), r->name, &(r->age), r->sex, r->email, r->phone, &(r->score));
}

void print_record(record* r)
{
	printf("record (index = %u, name = %s, age = %u, sex = %s, email = %s, phone = %s, score = %u, update = %s)\n",
		r->index, r->name, r->age, r->sex, r->email, r->phone, r->score, r->update);
}

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	tuple_def* def = get_new_tuple_def("students", 7, PAGE_SIZE);

	insert_element_def(def, "index", INT, 4, 0, NULL_USER_VALUE);
	insert_element_def(def, "name", VAR_STRING, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "age", UINT, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "sex", BIT_FIELD, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "email", VAR_STRING, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "phone", STRING, 14, 0, NULL_USER_VALUE);
	insert_element_def(def, "score", UINT, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "update", VAR_STRING, 1, 0, NULL_USER_VALUE);

	finalize_tuple_def(def);

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
	set_element_in_tuple(def, 3, tuple, &((user_value){.bit_field_value = ((strcmp(r->sex, "Male") == 0) ? 1 : 0)}));
	set_element_in_tuple(def, 4, tuple, &((user_value){.data = r->email, .data_size = strlen(r->email)}));
	set_element_in_tuple(def, 5, tuple, &((user_value){.data = r->phone, .data_size = strlen(r->phone)}));
	set_element_in_tuple(def, 6, tuple, &((user_value){.uint_value = r->score}));
	set_element_in_tuple(def, 7, tuple, &((user_value){.data = r->update, .data_size = strlen(r->update)}));
}

void read_record_from_tuple(record* r, const void* tupl, const tuple_def* tpl_d)
{
	r->index = get_value_from_element_from_tuple(tpl_d, 0, tupl).int_value;
	user_value name_data = get_value_from_element_from_tuple(tpl_d, 1, tupl);
	strncpy(r->name, name_data.data, name_data.data_size);
	r->age = get_value_from_element_from_tuple(tpl_d, 2, tupl).uint_value;
	uint8_t sex = 0;
	strcpy(r->sex, "Female");
	sex = get_value_from_element_from_tuple(tpl_d, 3, tupl).bit_field_value;
	if(sex)
		strcpy(r->sex, "Male");
	user_value email_data = get_value_from_element_from_tuple(tpl_d, 4, tupl);
	strncpy(r->email, email_data.data, email_data.data_size);
	user_value phone_data = get_value_from_element_from_tuple(tpl_d, 5, tupl);
	strncpy(r->phone, phone_data.data, phone_data.data_size);
	r->score = get_value_from_element_from_tuple(tpl_d, 6, tupl).uint_value;
	user_value update_data = get_value_from_element_from_tuple(tpl_d, 7, tupl);
	strncpy(r->update, update_data.data, update_data.data_size);
}

typedef struct result result;
struct result
{
	uint32_t operations_succeeded;
	uint32_t records_processed;
};

result insert_from_file(sorter_handle* sh_p, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process)
{
	// open test data file
	FILE* f = fopen(file_name, "r");

	result res = {};

	uint32_t records_seen = 0;
	while(!feof(f) && (tuples_to_process == 0 || res.records_processed < tuples_to_process))
	{
		// read a record from the file
		record r = {};
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
		char record_tuple[PAGE_SIZE] = {};
		build_tuple_from_record_struct(sh_p->std_p->record_def, record_tuple, &r);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

		// insert the record_tuple in the sorter rooted at root_page_id
		res.operations_succeeded += insert_in_sorter(sh_p, record_tuple, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// increment the tuples_processed count
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
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE, .system_header_size = SYSTEM_HEADER_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// construct tuple definitions for sorter
	sorter_tuple_defs std;
	init_sorter_tuple_definitions(&std, &(pam_p->pas), record_def, KEY_ELEMENTS_IN_RECORD, KEY_ELEMENTS_SORT_DIRECTION, KEY_ELEMENTS_COUNT);

	// print the generated sorter tuple defs
	print_sorter_tuple_definitions(&std);

	// create a sorter and get its root
	sorter_handle sh = get_new_sorter(&std, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// variable to test insert and delete operations
	result res;

	/* SETUP COMPLETED */

	/* INSERTIONS STARTED */

	res = insert_from_file(&sh, TEST_DATA_RANDOM_FILE, 0, 0, 256);
	printf("insertions to sorter completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	external_sort_merge_sorter(&sh, 3, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* CLEANUP */

	// destroy sorter, and fetch sorter
	uint64_t result_head_page_id;
	destroy_sorter(&sh, &result_head_page_id, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// print the result
	print_linked_page_list(result_head_page_id, &(std.lpltd), pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// destroy the result
	destroy_linked_page_list(result_head_page_id, &(std.lpltd), pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy sorter_tuple_definitions
	deinit_sorter_tuple_definitions(&std);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	// delete the record definition
	delete_tuple_def(record_def);

	return 0;
}