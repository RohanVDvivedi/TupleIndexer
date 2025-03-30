#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>

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

// below attribute must be between 1 and KEY_ELEMENTS_COUNT, both inclusive
// to beused only for find_from_file() functon
#define DEFAULT_FIND_KEY_COUNT_CONCERNED 1

// below attribute can be READ_LOCK, WRITE_LOCK or READ_LOCK_INTERIOR_WRITE_LOCK_LEAF
#define DEFAULT_FIND_LOCK_TYPE READ_LOCK
#define DEFAULT_FIND_IS_STACKED 0

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

void build_key_tuple_from_record_struct(const bplus_tree_tuple_defs* bpttd_p, void* key_tuple, const record* r)
{
	char record_tuple[PAGE_SIZE];
	build_tuple_from_record_struct(bpttd_p->record_def, record_tuple, r);
	extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(bpttd_p, record_tuple, key_tuple);
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

result insert_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_tree_after_each, int print_tree_on_completion, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
		build_tuple_from_record_struct(bpttd_p->record_def, record_tuple, &r);

		// printing built tuple
		//char print_buffer[PAGE_SIZE];
		//sprint_tuple(print_buffer, record_tuple, record_def);
		//printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(record_def, record_tuple), print_buffer);

		// insert the record_tuple in the bplus_tree rooted at root_page_id
		res.operations_succeeded += insert_in_bplus_tree(root_page_id, record_tuple, bpttd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// print bplus tree
		if(print_tree_after_each)
		{
			printf("---------------------------------------------------------------------------------------------------------\n");
			print_bplus_tree(root_page_id, 0, bpttd_p, pam_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			printf("---------------------------------------------------------------------------------------------------------\n\n");
		}

		// increment the tuples_processed count
		res.records_processed++;
	}

	// print bplus tree
	if(print_tree_on_completion)
	{
		print_bplus_tree(root_page_id, 1, bpttd_p, pam_p, transaction_id, &abort_error);
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

void print_all_forward(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	printf("printing all tuples\n");
	bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, MIN, DEFAULT_FIND_IS_STACKED, DEFAULT_FIND_LOCK_TYPE, bpttd_p, pam_p, ((DEFAULT_FIND_LOCK_TYPE == READ_LOCK) ? NULL : pmm_p), transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	const void* tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	while(tuple_to_print != NULL)
	{
		print_tuple(tuple_to_print, bpttd_p->record_def);
		next_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	}

	delete_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
	printf("\n");
}

int delete_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t tuples_to_process, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	uint32_t records_deleted = 0;

	// open test data file
	FILE* f = fopen(file_name, "r");

	record r = {};

	uint32_t records_seen = 0;
	while(!feof(f) && records_seen < skip_first)
	{
		// read a record from the file
		read_record_from_file(&r, f);
		records_seen++;
	}

	// close the file
	fclose(f);

	// open a stacked WRITE_LOCKed iterator at r and delete atleast tuples_to_process number of tuples
	{
		char key[PAGE_SIZE];
		build_key_tuple_from_record_struct(bpttd_p, key, &r);

		printf("deleting %u from key = ", tuples_to_process);
		print_tuple(key, bpttd_p->key_def);
		printf("\n");

		bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, key, KEY_ELEMENT_COUNT, GREATER_THAN_EQUALS, 1, WRITE_LOCK, bpttd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		const void* tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		while(tuple_to_process != NULL && records_deleted < tuples_to_process)
		{
			records_deleted += remove_from_bplus_tree_iterator(bpi_p, GO_NEXT_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		}

		delete_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		printf("\n");
	}

	return records_deleted;
}

int update_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t tuples_to_process, const char* last_col, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	uint32_t records_updated = 0;

	// open test data file
	FILE* f = fopen(file_name, "r");

	record r = {};

	uint32_t records_seen = 0;
	while(!feof(f) && records_seen < skip_first)
	{
		// read a record from the file
		read_record_from_file(&r, f);
		records_seen++;
	}

	// close the file
	fclose(f);

	// open a stacked WRITE_LOCKed iterator at r and delete atleast tuples_to_process number of tuples
	{
		char key[PAGE_SIZE];
		build_key_tuple_from_record_struct(bpttd_p, key, &r);

		printf("updating %u from key = ", tuples_to_process);
		print_tuple(key, bpttd_p->key_def);
		printf("\n");

		bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, key, KEY_ELEMENT_COUNT, GREATER_THAN_EQUALS, 1, WRITE_LOCK, bpttd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		const void* tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		while(tuple_to_process != NULL && records_updated < tuples_to_process)
		{
			char new_tuple[PAGE_SIZE];
			memory_move(new_tuple, tuple_to_process, get_tuple_size(bpttd_p->record_def, tuple_to_process));
			set_element_in_tuple(bpttd_p->record_def, STATIC_POSITION(7), new_tuple, &((user_value){.string_value = last_col, .string_size = strlen(last_col)}), UINT32_MAX);

			records_updated += update_at_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			next_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		}

		delete_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		printf("\n");
	}

	return records_updated;
}

int delete_all(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	uint32_t records_deleted = 0;

	// open a stacked WRITE_LOCKed iterator at r and delete atleast tuples_to_process number of tuples
	{
		printf("deleting all\n");
		bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, MIN, 1, WRITE_LOCK, bpttd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		const void* tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		while(tuple_to_process != NULL)
		{
			records_deleted += remove_from_bplus_tree_iterator(bpi_p, GO_NEXT_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		}

		delete_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		printf("\n");
	}

	return records_deleted;
}

int update_all(uint64_t root_page_id, const char* last_col, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	uint32_t records_updated = 0;

	// open a stacked WRITE_LOCKed iterator at r and delete atleast tuples_to_process number of tuples
	{
		printf("updating all\n");
		bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, MIN, 1, WRITE_LOCK, bpttd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		const void* tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		while(tuple_to_process != NULL)
		{
			char new_tuple[PAGE_SIZE];
			memory_move(new_tuple, tuple_to_process, get_tuple_size(bpttd_p->record_def, tuple_to_process));
			set_element_in_tuple(bpttd_p->record_def, STATIC_POSITION(7), new_tuple, &((user_value){.string_value = last_col, .string_size = strlen(last_col)}), UINT32_MAX);

			records_updated += update_at_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			next_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			tuple_to_process = get_tuple_bplus_tree_iterator(bpi_p);
		}

		delete_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		printf("\n");
	}

	return records_updated;
}

// find_pos for this function kust be MIN or MAX
void insert_at_ends_of_indexed_bplus_tree(uint64_t root_page_id, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	#ifndef KEY_INDEX_PHONE
		return ;
	#endif

	bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, find_pos, 1, WRITE_LOCK, bpttd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	char new_tuple[PAGE_SIZE];
	record r;

	// insert -1
	{
		r.index = -1;
		sprintf(r.name, "rohan->(%d)", r.index);
		r.age = 27;
		sprintf(r.sex, "Male");
		sprintf(r.email, "rohandvivedi@gmail.com");
		sprintf(r.phone, "9876543210");
		r.score = 101;
		sprintf(r.update, "update->(%d)", r.index);

		build_tuple_from_record_struct(bpttd_p->record_def, new_tuple, &r);

		int inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");

		inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");
	}

	// insert 0
	{
		r.index = 0;
		sprintf(r.name, "rohan->(%d)", r.index);
		r.age = 27;
		sprintf(r.sex, "Male");
		sprintf(r.email, "rohandvivedi@gmail.com");
		sprintf(r.phone, "9876543210");
		r.score = 101;
		sprintf(r.update, "update->(%d)", r.index);

		build_tuple_from_record_struct(bpttd_p->record_def, new_tuple, &r);

		int inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");

		inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");
	}

	// insert 258
	{
		r.index = 258;
		sprintf(r.name, "rohan->(%d)", r.index);
		r.age = 27;
		sprintf(r.sex, "Male");
		sprintf(r.email, "rohandvivedi@gmail.com");
		sprintf(r.phone, "9876543210");
		r.score = 101;
		sprintf(r.update, "update->(%d)", r.index);

		build_tuple_from_record_struct(bpttd_p->record_def, new_tuple, &r);

		int inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");

		inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");
	}

	// insert 257
	{
		r.index = 257;
		sprintf(r.name, "rohan->(%d)", r.index);
		r.age = 27;
		sprintf(r.sex, "Male");
		sprintf(r.email, "rohandvivedi@gmail.com");
		sprintf(r.phone, "9876543210");
		r.score = 101;
		sprintf(r.update, "update->(%d)", r.index);

		build_tuple_from_record_struct(bpttd_p->record_def, new_tuple, &r);

		int inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");

		inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");
	}

	// insert 259
	{
		r.index = 259;
		sprintf(r.name, "rohan->(%d)", r.index);
		r.age = 27;
		sprintf(r.sex, "Male");
		sprintf(r.email, "rohandvivedi@gmail.com");
		sprintf(r.phone, "9876543210");
		r.score = 101;
		sprintf(r.update, "update->(%d)", r.index);

		build_tuple_from_record_struct(bpttd_p->record_def, new_tuple, &r);

		int inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");

		inserted = insert_using_bplus_tree_iterator(bpi_p, new_tuple, 0, transaction_id, &abort_error);

		printf("result %d of insertion for ", inserted);
		print_tuple(new_tuple, bpttd_p->record_def);
		printf("\n");
	}

	delete_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
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

	// construct tuple definitions for bplus_tree
	bplus_tree_tuple_defs bpttd;
	init_bplus_tree_tuple_definitions(&bpttd, &(pam_p->pas), record_def, KEY_ELEMENTS_IN_RECORD, KEY_ELEMENTS_SORT_DIRECTION, KEY_ELEMENTS_COUNT);

	// print the generated bplus tree tuple defs
	print_bplus_tree_tuple_definitions(&bpttd);

	// create a bplus tree and get its root
	uint64_t root_page_id = get_new_bplus_tree(&bpttd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// variable to test insert and delete operations
	result res;

	/* SETUP COMPLETED */
	printf("\n");

	// again try insert all tuples now 64/256 tuples must fail from TEST_DATA_RANDOM_FILE
	/* INSERTIONS STARTED */

	res = insert_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 0, &bpttd, pam_p, pmm_p);

	printf("insertions to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* INSERTIONS COMPLETED */

	/* TEST ITERATOR BASED DELETION */

	delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 15, 15, &bpttd, pam_p, pmm_p);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* TEST ITERATOR BASED DELETION */

	delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 19, 15, &bpttd, pam_p, pmm_p);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* TEST ITERATOR BASED DELETION */

	delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 40, 15, &bpttd, pam_p, pmm_p);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* TEST ITERATOR BASED UPDATION */

	update_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 238, 15, "XXX", &bpttd, pam_p, pmm_p);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* TEST ITERATOR BASED UPDATION */

	update_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 233, 15, "VVVVWWWWXXXXYYYYZZZZZZVVVVWWWWXXXXYYYYZZZZZZVVVVWWWWXXXX", &bpttd, pam_p, pmm_p);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* UPDATE ALL */
	update_all(root_page_id, "VVVVWWWWXXXXYYYYZZZZZZVVVVWWWWXXXXYYYYZZZZZZVVVVWWWWXXXX", &bpttd, pam_p, pmm_p);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* INSERT AT ENDS*/
	#ifdef KEY_INDEX_PHONE
		insert_at_ends_of_indexed_bplus_tree(root_page_id, MIN, &bpttd, pam_p, pmm_p);
		insert_at_ends_of_indexed_bplus_tree(root_page_id, MAX, &bpttd, pam_p, pmm_p);
		print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);
	#endif

	/* DELETE ALL */

	delete_all(root_page_id, &bpttd, pam_p, pmm_p);

	print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);

	/* INSERT AT ENDS*/
	#ifdef KEY_INDEX_PHONE
		insert_at_ends_of_indexed_bplus_tree(root_page_id, MIN, &bpttd, pam_p, pmm_p);
		insert_at_ends_of_indexed_bplus_tree(root_page_id, MAX, &bpttd, pam_p, pmm_p);
		print_all_forward(root_page_id, &bpttd, pam_p, pmm_p);
	#endif

	/* PRINTING EMPTY TREE */

	printf("printing empty tree for you\n");
	print_bplus_tree(root_page_id, 1, &bpttd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
	printf("\n");

	/* CLEANUP */

	// destroy bplus tree
	destroy_bplus_tree(root_page_id, &bpttd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy bplus_tree_tuple_definitions
	deinit_bplus_tree_tuple_definitions(&bpttd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	// delete the record definition

	return 0;
}