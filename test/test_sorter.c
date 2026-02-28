#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/sorter/sorter.h>
#include<tupleindexer/linked_page_list/linked_page_list.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

// uncomment based on the keys that you want to test with
#define KEY_NAME_EMAIL
//#define KEY_INDEX_PHONE
//#define KEY_PHONE_SCORE
//#define KEY_EMAIL_AGE_SEX
//#define KEY_SEX_EMAIL
//#define KEY_SCORE_INDEX
//#define KEY_SCORE_NAME
//#define KEY_NAME0_EMAIL0_PHONE1 // key is first character of name, frist character of email, second character of phone number

#define TEST_DATA_FILE         "./testdata.csv"
#define TEST_DATA_RANDOM_FILE  "./testdata_random.csv"

#if defined KEY_NAME_EMAIL
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(1), STATIC_POSITION(4)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_INDEX_PHONE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(0), STATIC_POSITION(5)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_PHONE_SCORE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(5), STATIC_POSITION(6)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_EMAIL_AGE_SEX
	#define KEY_ELEMENTS_COUNT			3
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(4), STATIC_POSITION(2), STATIC_POSITION(3)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC,ASC}
#elif defined KEY_SEX_EMAIL
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(3), STATIC_POSITION(4)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_SCORE_INDEX
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(6), STATIC_POSITION(0)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_SCORE_NAME
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(6), STATIC_POSITION(1)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC}
#elif defined KEY_NAME0_EMAIL0_PHONE1
	#define KEY_ELEMENTS_COUNT			3
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(1,0), STATIC_POSITION(4,0), STATIC_POSITION(5,1)}
	#define KEY_ELEMENTS_SORT_DIRECTION (compare_direction []){ASC,ASC,ASC}
#endif

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

#define N_WAY_SORT 3

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

tuple_def tuple_definition;
char tuple_type_info_memory[sizeof_tuple_data_type_info(8)];
data_type_info* tuple_type_info = (data_type_info*)tuple_type_info_memory;
data_type_info c1_type_info;
data_type_info c4_type_info;
data_type_info c5_type_info;
data_type_info c7_type_info;

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	initialize_tuple_data_type_info(tuple_type_info, "students", 1, PAGE_SIZE, 8);

	strcpy(tuple_type_info->containees[0].field_name, "index");
	tuple_type_info->containees[0].al.type_info = INT_NULLABLE[4];

	c1_type_info = get_variable_length_string_type("", 256);
	strcpy(tuple_type_info->containees[1].field_name, "name");
	tuple_type_info->containees[1].al.type_info = &c1_type_info;

	strcpy(tuple_type_info->containees[2].field_name, "age");
	tuple_type_info->containees[2].al.type_info = UINT_NULLABLE[1];

	strcpy(tuple_type_info->containees[3].field_name, "sex");
	tuple_type_info->containees[3].al.type_info = BIT_FIELD_NULLABLE[1];

	c4_type_info = get_variable_length_string_type("", 256);
	strcpy(tuple_type_info->containees[4].field_name, "email");
	tuple_type_info->containees[4].al.type_info = &c4_type_info;

	c5_type_info = get_fixed_length_string_type("", 14, 1);
	strcpy(tuple_type_info->containees[5].field_name, "phone");
	tuple_type_info->containees[5].al.type_info = &c5_type_info;

	strcpy(tuple_type_info->containees[6].field_name, "score");
	tuple_type_info->containees[6].al.type_info = UINT_NULLABLE[1];

	c7_type_info = get_variable_length_string_type("", 256);
	strcpy(tuple_type_info->containees[7].field_name, "update");
	tuple_type_info->containees[7].al.type_info = &c7_type_info;

	if(!initialize_tuple_def(&tuple_definition, tuple_type_info))
	{
		printf("failed finalizing tuple definition\n");
		exit(-1);
	}

	print_tuple_def(&tuple_definition);
	printf("\n\n");

	return &tuple_definition;
}

void build_tuple_from_record_struct(const tuple_def* def, void* tuple, const record* r)
{
	init_tuple(def, tuple);

	set_element_in_tuple(def, STATIC_POSITION(0), tuple, &((datum){.int_value = r->index}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(1), tuple, &((datum){.string_value = r->name, .string_size = strlen(r->name)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(2), tuple, &((datum){.uint_value = r->age}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(3), tuple, &((datum){.bit_field_value = ((strcmp(r->sex, "Male") == 0) ? 1 : 0)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(4), tuple, &((datum){.string_value = r->email, .string_size = strlen(r->email)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(5), tuple, &((datum){.string_value = r->phone, .string_size = strlen(r->phone)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(6), tuple, &((datum){.uint_value = r->score}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(7), tuple, &((datum){.string_value = r->update, .string_size = strlen(r->update)}), UINT32_MAX);
}

void read_record_from_tuple(record* r, const void* tupl, const tuple_def* tpl_d)
{
	datum uval;
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(0), tupl);
	r->index = uval.int_value;
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(1), tupl);
	strncpy(r->name, uval.string_value, uval.string_size);
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(2), tupl);
	r->age = uval.uint_value;
	strcpy(r->sex, "Female");
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(3), tupl);
	if(uval.bit_field_value)
		strcpy(r->sex, "Male");
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(4), tupl);
	strncpy(r->email, uval.string_value, uval.string_size);
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(5), tupl);
	strncpy(r->phone, uval.string_value, uval.string_size);
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(6), tupl);
	r->score = uval.uint_value;
	get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(7), tupl);
	strncpy(r->update, uval.string_value, uval.string_size);
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

void evaluate_sort_result(uint64_t result_head_page_id, uint64_t tuples_count)
{
	uint64_t tuples_seen = 0;

	if(tuples_seen != tuple_count)
		printf("sorting evaluation expected %"PRIu64" tuples, but we instead found %"PRIu64"\n\n", tuples_count, tuples_seen);
}

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE}));

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
	uint64_t tuples_count = 0;

	/* SETUP COMPLETED */

	/* INSERTIONS STARTED */

	res = insert_from_file(&sh, TEST_DATA_FILE, 0, 0, 256);
	tuples_count += res.operations_succeeded;
	printf("insertions to sorter completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = insert_from_file(&sh, TEST_DATA_RANDOM_FILE, 0, 4, 256);
	tuples_count += res.operations_succeeded;
	printf("insertions to sorter completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	insert_in_sorter(&sh, NULL, transaction_id, &abort_error);
	printf("tuple flushing from buffer done\n\n");

	int merge_result = 0;
	do
	{
		merge_result = merge_N_runs_in_sorter(&sh, N_WAY_SORT, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}while(merge_result);

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

	// evaluate sorting results
	evaluate_sort_result(result_head_page_id, tuples_count);

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

	return 0;
}