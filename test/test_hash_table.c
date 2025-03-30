#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/hash_table/hash_table.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

#define INITIAL_BUCKET_COUNT 19

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
#elif defined KEY_INDEX_PHONE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(0), STATIC_POSITION(5)}
#elif defined KEY_PHONE_SCORE
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(5), STATIC_POSITION(6)}
#elif defined KEY_EMAIL_AGE_SEX
	#define KEY_ELEMENTS_COUNT			3
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(4), STATIC_POSITION(2), STATIC_POSITION(3)}
#elif defined KEY_SEX_EMAIL
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(3), STATIC_POSITION(4)}
#elif defined KEY_SCORE_INDEX
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(6), STATIC_POSITION(0)}
#elif defined KEY_SCORE_NAME
	#define KEY_ELEMENTS_COUNT			2
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(6), STATIC_POSITION(1)}
#elif defined KEY_NAME0_EMAIL0_PHONE1
	#define KEY_ELEMENTS_COUNT			3
	#define KEY_ELEMENTS_IN_RECORD 		(positional_accessor []){STATIC_POSITION(1,0), STATIC_POSITION(4,0), STATIC_POSITION(5,1)}
#endif

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

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

	set_element_in_tuple(def, STATIC_POSITION(0), tuple, &((user_value){.int_value = r->index}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(1), tuple, &((user_value){.string_value = r->name, .string_size = strlen(r->name)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(2), tuple, &((user_value){.uint_value = r->age}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(3), tuple, &((user_value){.bit_field_value = ((strcmp(r->sex, "Male") == 0) ? 1 : 0)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(4), tuple, &((user_value){.string_value = r->email, .string_size = strlen(r->email)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(5), tuple, &((user_value){.string_value = r->phone, .string_size = strlen(r->phone)}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(6), tuple, &((user_value){.uint_value = r->score}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(7), tuple, &((user_value){.string_value = r->update, .string_size = strlen(r->update)}), UINT32_MAX);
}

void build_key_tuple_from_record_struct(const hash_table_tuple_defs* httd_p, void* key_tuple, const record* r)
{
	char record_tuple[PAGE_SIZE];
	build_tuple_from_record_struct(httd_p->lpltd.record_def, record_tuple, r);
	extract_key_from_record_tuple_using_hash_table_tuple_definitions(httd_p, record_tuple, key_tuple);
}

void read_record_from_tuple(record* r, const void* tupl, const tuple_def* tpl_d)
{
	user_value uval;
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

#define MIN_THRESHOLD -3
#define MAX_THRESHOLD 10
int fill_figure = 0;

// after every insert, check and increment fill_figure if the bucket is full,
// and decrement after every delete, that makes the bucket empty
// if the fill_figure > THRESHOLD, then expand and reset fill_figure
// if the fill_figure < -THRESHOLD, then shrink and reset fill_figure

result insert_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_on_completion, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
		build_tuple_from_record_struct(httd_p->lpltd.record_def, record_tuple, &r);
		char key_tuple[PAGE_SIZE] = {};
		build_key_tuple_from_record_struct(httd_p, key_tuple, &r);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

		//printf("hash = %"PRIu64"\n", hash_tuple(record_tuple, httd_p->lpltd.record_def, httd_p->key_element_ids, hash_func, httd_p->key_element_count));

		hash_table_iterator* hti_p = get_new_hash_table_iterator(root_page_id, (bucket_range){}, key_tuple, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// insert the record_tuple in the hash_table
		int rest = insert_in_hash_table_iterator(hti_p, record_tuple, transaction_id, &abort_error);
		res.operations_succeeded += rest;
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		if(rest && is_curr_bucket_full_for_hash_table_iterator(hti_p))
			fill_figure++;

		hash_table_vaccum_params htvp;
		delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// no vaccum required here since we are only performing an insert

		if(fill_figure > MAX_THRESHOLD)
		{
			fill_figure = 0;
			int expanded = expand_hash_table(root_page_id, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(expanded)
				printf("EXPANDED\n");
			else
				printf("EXPAND FAILED");
		}

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print hash table
	if(print_on_completion)
	{
		print_hash_table(root_page_id, httd_p, pam_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	// close the file
	fclose(f);

	return res;
}

result insert_unique_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_on_completion, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
		build_tuple_from_record_struct(httd_p->lpltd.record_def, record_tuple, &r);
		char key_tuple[PAGE_SIZE] = {};
		build_key_tuple_from_record_struct(httd_p, key_tuple, &r);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

		//printf("hash = %"PRIu64"\n", hash_tuple(record_tuple, httd_p->lpltd.record_def, httd_p->key_element_ids, hash_func, httd_p->key_element_count));

		hash_table_iterator* hti_p = get_new_hash_table_iterator(root_page_id, (bucket_range){}, key_tuple, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		int found = 0;

		// go next until you find a match
		while(1)
		{
			const void* curr_seen = get_tuple_hash_table_iterator(hti_p);
			if(curr_seen != NULL)
			{
				found = 1;
				break;
			}

			int next_res = next_hash_table_iterator(hti_p, 0, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			if(next_res == 0)
				break;
		}

		int rest = 0;

		if(!found)
		{
			// insert the record_tuple in the hash_table
			rest = insert_in_hash_table_iterator(hti_p, record_tuple, transaction_id, &abort_error);
			res.operations_succeeded += rest;
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}

		if(rest && is_curr_bucket_full_for_hash_table_iterator(hti_p))
			fill_figure++;

		hash_table_vaccum_params htvp;
		delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		if(fill_figure > MAX_THRESHOLD)
		{
			fill_figure = 0;
			int expanded = expand_hash_table(root_page_id, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(expanded)
				printf("EXPANDED\n");
			else
				printf("EXPAND FAILED");
		}

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print hash table
	if(print_on_completion)
	{
		print_hash_table(root_page_id, httd_p, pam_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	// close the file
	fclose(f);

	return res;
}

result update_non_key_element_in_file(uint64_t root_page_id, char* element, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_on_completion, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
		build_tuple_from_record_struct(httd_p->lpltd.record_def, record_tuple, &r);
		char key_tuple[PAGE_SIZE] = {};
		build_key_tuple_from_record_struct(httd_p, key_tuple, &r);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

		hash_table_iterator* hti_p = get_new_hash_table_iterator(root_page_id, (bucket_range){}, key_tuple, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// go next until you can
		while(1)
		{
			res.operations_succeeded += update_non_key_element_in_place_at_hash_table_iterator(hti_p, STATIC_POSITION(7), &((user_value){.string_value = element, .string_size = strlen(element)}), transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			int next_res = next_hash_table_iterator(hti_p, 0, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			if(next_res == 0)
				break;
		}

		hash_table_vaccum_params htvp;
		delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// no vaccum required here since we are only updating an element of tuple in place

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print hash table
	if(print_on_completion)
	{
		print_hash_table(root_page_id, httd_p, pam_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	// close the file
	fclose(f);

	return res;
}

result update_in_file(uint64_t root_page_id, char* element, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_on_completion, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
		build_tuple_from_record_struct(httd_p->lpltd.record_def, record_tuple, &r);
		char key_tuple[PAGE_SIZE] = {};
		build_key_tuple_from_record_struct(httd_p, key_tuple, &r);

		set_element_in_tuple(httd_p->lpltd.record_def, STATIC_POSITION(7), record_tuple, &((user_value){.string_value = element, .string_size = strlen(element)}), UINT32_MAX);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

		hash_table_iterator* hti_p = get_new_hash_table_iterator(root_page_id, (bucket_range){}, key_tuple, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// go next until you can
		while(1)
		{
			res.operations_succeeded += update_at_hash_table_iterator(hti_p, record_tuple, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			int next_res = next_hash_table_iterator(hti_p, 0, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			if(next_res == 0)
				break;
		}

		hash_table_vaccum_params htvp;
		delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// no vaccum required here since we are only updating an element of tuple in place

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print hash table
	if(print_on_completion)
	{
		print_hash_table(root_page_id, httd_p, pam_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	// close the file
	fclose(f);

	return res;
}

result delete_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_on_completion, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
		build_tuple_from_record_struct(httd_p->lpltd.record_def, record_tuple, &r);
		char key_tuple[PAGE_SIZE] = {};
		build_key_tuple_from_record_struct(httd_p, key_tuple, &r);

		// printing built key_tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, key_tuple, bpttd.key_def);
		//printf("Built key_tuple : size(%u)\n\t%s\n\n", get_tuple_size(bpttd.key_def, key_tuple), print_buffer);

		hash_table_iterator* hti_p = get_new_hash_table_iterator(root_page_id, (bucket_range){}, key_tuple, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// go next until you can
		while(1)
		{
			int removed = remove_from_hash_table_iterator(hti_p, transaction_id, &abort_error);
			res.operations_succeeded += removed;
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			if(removed && is_curr_bucket_empty_for_hash_table_iterator(hti_p))
				fill_figure--;

			if(removed)
				continue;

			int next_res = next_hash_table_iterator(hti_p, 0, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			if(next_res == 0)
				break;
		}

		hash_table_vaccum_params htvp;
		delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// here since we deleted using a key, we might have emptied a bucket, hence a vaccum is necessary
		perform_vaccum_hash_table(root_page_id, &htvp, 1, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		if(fill_figure < MIN_THRESHOLD)
		{
			fill_figure = 0;
			int shrunk = shrink_hash_table(root_page_id, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(shrunk)
				printf("SHRUNK\n");
			else
				printf("SHRINK FAILED\n");
		}

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print hash table
	if(print_on_completion)
	{
		print_hash_table(root_page_id, httd_p, pam_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	// close the file
	fclose(f);

	return res;
}

int delete_all_from_hash_table(uint64_t root_page_id, uint64_t start_bucket_id, uint64_t last_bucket_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	int removed_count = 0;

	hash_table_iterator* hti_p = get_new_hash_table_iterator(root_page_id, (bucket_range){start_bucket_id, last_bucket_id}, NULL, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// go next until you can
	while(1)
	{
		int removed = remove_from_hash_table_iterator(hti_p, transaction_id, &abort_error);
		removed_count += removed;
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		if(removed)
			continue;

		int next_res = next_hash_table_iterator(hti_p, 1, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		if(next_res == 0)
			break;
	}

	hash_table_vaccum_params htvp;
	delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// here since we deleted using a key, we might have emptied a bucket, hence a vaccum is necessary
	perform_vaccum_hash_table(root_page_id, &htvp, 1, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	return removed_count;
}

void print_2_buckets(uint64_t root_page_id, uint64_t start_bucket_id, const hash_table_tuple_defs* httd_p, const page_access_methods* pam_p)
{
	hash_table_iterator* hti1_p = get_new_hash_table_iterator(root_page_id, (bucket_range){start_bucket_id, start_bucket_id + 1}, NULL, httd_p, pam_p, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	hash_table_iterator* hti2_p = clone_hash_table_iterator(hti1_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	while(start_bucket_id == get_curr_bucket_index_for_hash_table_iterator(hti2_p))
	{
		int next_res = next_hash_table_iterator(hti2_p, 1, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		if(next_res == 0)
			break;
	}

	printf("bucket = %"PRIu64"\n", get_curr_bucket_index_for_hash_table_iterator(hti1_p));
	while(1)
	{
		const void* tuple = get_tuple_hash_table_iterator(hti1_p);
		if(tuple == NULL)
			printf("NULL");
		else
			print_tuple(tuple, httd_p->lpltd.record_def);

		int next_res = next_hash_table_iterator(hti1_p, 0, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		if(next_res == 0)
			break;
	}
	printf("\n");

	printf("bucket = %"PRIu64"\n", get_curr_bucket_index_for_hash_table_iterator(hti2_p));
	while(1)
	{
		const void* tuple = get_tuple_hash_table_iterator(hti2_p);
		if(tuple == NULL)
			printf("NULL");
		else
			print_tuple(tuple, httd_p->lpltd.record_def);

		int next_res = next_hash_table_iterator(hti2_p, 0, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		if(next_res == 0)
			break;
	}

	hash_table_vaccum_params htvp;

	delete_hash_table_iterator(hti1_p, &htvp, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_hash_table_iterator(hti2_p, &htvp, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// no vaccum required here, since we are only reading data

	printf("\n\n");
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

	// construct tuple definitions for hash_table
	hash_table_tuple_defs httd;
	init_hash_table_tuple_definitions(&httd, &(pam_p->pas), record_def, KEY_ELEMENTS_IN_RECORD, KEY_ELEMENTS_COUNT, FNV_64_TUPLE_HASHER);

	// print the generated bplus tree tuple defs
	print_hash_table_tuple_definitions(&httd);

	// create a bplus tree and get its root
	uint64_t root_page_id = get_new_hash_table(INITIAL_BUCKET_COUNT, &httd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* SETUP COMPLETED */
	printf("\n");

	// print the constructed page table
	print_hash_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);

	/* TESTS STARTED */

	result res;

	// insert all tuples
	res = insert_from_file(root_page_id, TEST_DATA_FILE, 3, 3, 256, 0, &httd, pam_p, pmm_p);

	printf("insertions to hash table completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	// insert all tuples
	res = insert_unique_from_file(root_page_id, TEST_DATA_FILE, 0, 0, 256, 0, &httd, pam_p, pmm_p);

	printf("insertions to hash table completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	// ---------------------------
	int shrink_res = shrink_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("shrink result = %d\n\n", shrink_res);

	shrink_res = shrink_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("shrink result = %d\n\n", shrink_res);

	shrink_res = shrink_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("shrink result = %d\n\n", shrink_res);
	// ---------------------------

	int expand_res = expand_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("expand result = %d\n\n", expand_res);

	expand_res = expand_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("expand result = %d\n\n", expand_res);

	expand_res = expand_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("expand result = %d\n\n", expand_res);

	expand_res = expand_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("expand result = %d\n\n", expand_res);

	expand_res = expand_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("expand result = %d\n\n", expand_res);

	expand_res = expand_hash_table(root_page_id, &httd, pam_p, pmm_p, transaction_id, &abort_error);

	printf("expand result = %d\n\n", expand_res);
	// ---------------------------

	res = update_in_file(root_page_id, "ABCDEF", TEST_DATA_FILE, 16, 0, 10, 0, &httd, pam_p, pmm_p);

	printf("update to ABCDEF in hash table completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_non_key_element_in_file(root_page_id, "ABC", TEST_DATA_FILE, 19, 0, 5, 0, &httd, pam_p, pmm_p);

	printf("update_non_key to ABC in hash table completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	// print the constructed page table
	print_hash_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);

	print_2_buckets(root_page_id, 10, &httd, pam_p);

	res = delete_from_file(root_page_id, TEST_DATA_FILE, 3, 3, 256, 0, &httd, pam_p, pmm_p);

	printf("deletions to hash table completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

#define DELETE_ALL_USING_FILE

#ifdef DELETE_ALL_USING_FILE
	res = delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, &httd, pam_p, pmm_p);

	printf("deletions to hash table completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);
#else
	int rem_count = delete_all_from_hash_table(root_page_id, 0, 19, &httd, pam_p, pmm_p);

	printf("delete all deleted %d tuples\n\n", rem_count);
#endif

	// print the constructed page table
	print_hash_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);

	/* TESTS ENDED */

	/* CLEANUP */

	// destroy bplus tree
	destroy_hash_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy page_table_tuple_definitions
	deinit_hash_table_tuple_definitions(&httd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	// delete the record definition

	return 0;
}