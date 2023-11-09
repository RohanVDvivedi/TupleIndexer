#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuple.h>
#include<tuple_def.h>

#include<bplus_tree.h>
#include<bplus_tree_tuple_definitions.h>
#include<data_access_methods.h>
#include<unWALed_in_memory_data_store.h>
#include<unWALed_page_modification_methods.h>

// uncomment based on the keys that you want to test with
#define KEY_NAME_EMAIL
//#define KEY_INDEX_PHONE
//#define KEY_PHONE_SCORE
//#define KEY_EMAIL_AGE_SEX

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
#endif

#define DEFAULT_COMMON_PAGE_HEADER_SIZE 3

#define PAGE_SIZE 256

#define PAGE_ID_WIDTH 3

#define DEFAULT_FIND_LEAF_LOCK_TYPE READ_LOCK
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

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	tuple_def* def = get_new_tuple_def("students", 7, PAGE_SIZE);

	insert_element_def(def, "index", INT, 4, 0, NULL_USER_VALUE);
	insert_element_def(def, "name", VAR_STRING, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "age", UINT, 1, 0, NULL_USER_VALUE);
	insert_element_def(def, "sex", UINT, 1, 0, NULL_USER_VALUE);
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
	set_element_in_tuple(def, 3, tuple, &((user_value){.uint_value = ((strcmp(r->sex, "Male") == 0) ? 1 : 0)}));
	set_element_in_tuple(def, 4, tuple, &((user_value){.data = r->email, .data_size = strlen(r->email)}));
	set_element_in_tuple(def, 5, tuple, &((user_value){.data = r->phone, .data_size = strlen(r->phone)}));
	set_element_in_tuple(def, 6, tuple, &((user_value){.uint_value = r->score}));
	set_element_in_tuple(def, 7, tuple, &((user_value){.data = r->update, .data_size = strlen(r->update)}));
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
	user_value update_data = get_value_from_element_from_tuple(tpl_d, 7, tupl);
	strncpy(r->update, update_data.data, update_data.data_size);
}

typedef struct result result;
struct result
{
	uint32_t operations_succeeded;
	uint32_t records_processed;
};

result insert_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_tree_after_each, int print_tree_on_completion, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
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
		res.operations_succeeded += insert_in_bplus_tree(root_page_id, record_tuple, bpttd_p, dam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// print bplus tree
		if(print_tree_after_each)
		{
			printf("---------------------------------------------------------------------------------------------------------\n");
			print_bplus_tree(root_page_id, 0, bpttd_p, dam_p, transaction_id, &abort_error);
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
		print_bplus_tree(root_page_id, 1, bpttd_p, dam_p, transaction_id, &abort_error);
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

// insert -> update only if the old_record is NULL, i.e. being an insert
int inserted_update_inspect(const void* context, const tuple_def* record_def, const void* old_record, void** new_record, void (*cancel_update_callback)(void* cancel_update_callback_context, const void* transaction_id, int* abort_error), void* cancel_update_callback_context, const void* transaction_id, int* abort_error)
{
	if(old_record == NULL)
		return 1;
	else
		return 0;
}

update_inspector ii = {
	.context = NULL,
	.update_inspect = inserter_update_inspect,
};

int updater_update_inspect(const void* context, const tuple_def* record_def, const void* old_record, void** new_record, void (*cancel_update_callback)(void* cancel_update_callback_context, const void* transaction_id, int* abort_error), void* cancel_update_callback_context, const void* transaction_id, int* abort_error)
{
	user_value update_data = get_value_from_element_from_tuple(record_def, 7, old_record);
	char update_value[64] = {};
	strncpy(update_value, update_data.data, update_data.data_size);
	if(strlen(update_value) == 0)
		update_value[0] = 'A';
	else if(strlen(update_value) == 3)
		strcpy(update_value, "R");
	else
		update_value[strlen(update_value)] = update_value[strlen(update_value)-1] + 1;
	set_element_in_tuple(record_def, 7, *new_record, &((user_value){.data = update_value, .data_size = strlen(update_value)}));

	return 1;
}

update_inspector ui = {
	.context = NULL,
	.update_inspect = updater_update_inspect,
};

int deletor_update_inspect(const void* context, const tuple_def* record_def, const void* old_record, void** new_record, void (*cancel_update_callback)(void* cancel_update_callback_context, const void* transaction_id, int* abort_error), void* cancel_update_callback_context, const void* transaction_id, int* abort_error)
{
	(*new_record) = NULL;
	return 1;
}

update_inspector di = {
	.context = NULL,
	.update_inspect = deletor_update_inspect,
};

int reader_update_inspect(const void* context, const tuple_def* record_def, const void* old_record, void** new_record, void (*cancel_update_callback)(void* cancel_update_callback_context, const void* transaction_id, int* abort_error), void* cancel_update_callback_context, const void* transaction_id, int* abort_error)
{
	cancel_update_callback(cancel_update_callback_context, transaction_id, abort_error);
	if(old_record == NULL)
		printf("NOT FOUND\n");
	else
		print_tuple(old_record, record_def);
	return 0;
}

update_inspector ri = {
	.context = NULL,
	.update_inspect = reader_update_inspect,
};

result update_in_file(uint64_t root_page_id, const update_inspector* ui, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_tree_after_each, int print_tree_on_completion, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
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
		res.operations_succeeded += inspected_update_in_bplus_tree(root_page_id, record_tuple, ui, bpttd_p, dam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// print bplus tree
		if(print_tree_after_each)
		{
			printf("---------------------------------------------------------------------------------------------------------\n");
			print_bplus_tree(root_page_id, 0, bpttd_p, dam_p, transaction_id, &abort_error);
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
		print_bplus_tree(root_page_id, 1, bpttd_p, dam_p, transaction_id, &abort_error);
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

result delete_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, int print_tree_after_each, int print_tree_on_completion, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
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
		res.operations_succeeded += delete_from_bplus_tree(root_page_id, key_tuple, bpttd_p, dam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		// print bplus tree
		if(print_tree_after_each)
		{
			printf("---------------------------------------------------------------------------------------------------------\n");
			print_bplus_tree(root_page_id, 0, bpttd_p, dam_p, transaction_id, &abort_error);
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
		print_bplus_tree(root_page_id, 1, bpttd_p, dam_p, transaction_id, &abort_error);
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

result find_from_file(uint64_t root_page_id, char* file_name, uint32_t skip_first, uint32_t skip_every, uint32_t tuples_to_process, uint32_t max_scan_length, uint32_t key_element_count_concerned, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// open test data file
	FILE* f = fopen(file_name, "r");

	result res = {};

	printf("printing the first 4 tuples\n");

	bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, GREATER_THAN, DEFAULT_FIND_LEAF_LOCK_TYPE, DEFAULT_FIND_IS_STACKED, bpttd_p, dam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	const void* tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	uint32_t tuples_to_print = 0;
	while(tuple_to_print != NULL && tuples_to_print < max_scan_length)
	{
		print_tuple(tuple_to_print, bpttd_p->record_def);
		tuples_to_print++;
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

	printf("printing the last 4 tuples\n");

	bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, LESSER_THAN, DEFAULT_FIND_LEAF_LOCK_TYPE, DEFAULT_FIND_IS_STACKED, bpttd_p, dam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	tuple_to_print = get_tuple_bplus_tree_iterator(bpi_p);
	tuples_to_print = 0;
	while(tuple_to_print != NULL && tuples_to_print < max_scan_length)
	{
		print_tuple(tuple_to_print, bpttd_p->record_def);
		tuples_to_print++;
		prev_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
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

		bpi_p = find_in_bplus_tree(root_page_id, key_tuple, key_element_count_concerned, find_pos, DEFAULT_FIND_LEAF_LOCK_TYPE, DEFAULT_FIND_IS_STACKED, bpttd_p, dam_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

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
					prev_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
					if(abort_error)
					{
						printf("ABORTED\n");
						exit(-1);
					}
					break;
				}
				case GREATER_THAN_EQUALS :
				case GREATER_THAN :
				{
					next_bplus_tree_iterator(bpi_p, transaction_id, &abort_error);
					if(abort_error)
					{
						printf("ABORTED\n");
						exit(-1);
					}
					break;
				}
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
	data_access_methods* dam_p = get_new_unWALed_in_memory_data_store(PAGE_SIZE, PAGE_ID_WIDTH);

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// construct tuple definitions for bplus_tree
	bplus_tree_tuple_defs bpttd;
	init_bplus_tree_tuple_definitions(&bpttd, DEFAULT_COMMON_PAGE_HEADER_SIZE, record_def, KEY_ELEMENTS_IN_RECORD, KEY_ELEMENTS_SORT_DIRECTION, KEY_ELEMENTS_COUNT, PAGE_SIZE, PAGE_ID_WIDTH, dam_p->NULL_PAGE_ID);

	// print the generated bplus tree tuple defs
	print_bplus_tree_tuple_definitions(&bpttd);

	// create a bplus tree and get its root
	uint64_t root_page_id = get_new_bplus_tree(&bpttd, dam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// variable to test insert and delete operations
	result res;

	/* SETUP COMPLETED */
	printf("\n");



	// insert every 4th tuple from TEST_DATA_FILE
	/* INSERTIONS STARTED */

	res = insert_from_file(root_page_id, TEST_DATA_FILE, 0, 3, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("insertions to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* INSERTIONS COMPLETED */



	// again try insert all tuples now 64/256 tuples must fail from TEST_DATA_RANDOM_FILE
	/* INSERTIONS STARTED */

	res = insert_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("insertions to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* INSERTIONS COMPLETED */


	// perfrom find 12 times on tules from TEST_DATA_FILE on all tuples
	/* FIND STARTED */

	res = find_from_file(root_page_id, TEST_DATA_FILE, 3, 5, 12, 64, 1, &bpttd, dam_p);

	printf("finds in bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* FIND COMPLETED */


	// delete some random 30 tuples from TEST_DATA_RANDOM_FILE -> 30/30
	/* DELETIONS STARTED */

	res = delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 1, 6, 30, 0, 0, &bpttd, dam_p, pmm_p);

	printf("deletions to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* DELETIONS COMPLETED */



	// perfrom find on remaining tuples
	/* FIND STARTED */

	res = find_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 1, 6, 9, 64, 1, &bpttd, dam_p);

	printf("finds in bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* FIND COMPLETED */




	// delete some random 30 tuples from TEST_DATA_RANDOM_FILE -> lesser than 32 success
	/* DELETIONS STARTED */

	res = delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 7, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("deletions to bplus tree completed(%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* DELETIONS COMPLETED */




	// perfrom find on remaining tuples
	/* FIND STARTED */

	res = find_from_file(root_page_id, TEST_DATA_FILE, 3, 3, 9, 64, 1, &bpttd, dam_p);

	printf("finds in bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* FIND COMPLETED */



	// again insert all from TEST_DATA_RANDOM_FILE -> lesser than 62 failures
	/* INSERTIONS STARTED */

	res = insert_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("insertions to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* INSERTIONS COMPLETED */


	/* UPDATES */

	res = update_in_file(root_page_id, &ui, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_in_file(root_page_id, &ui, TEST_DATA_RANDOM_FILE, 0, 1, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_in_file(root_page_id, &ui, TEST_DATA_RANDOM_FILE, 0, 3, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_in_file(root_page_id, &ui, TEST_DATA_RANDOM_FILE, 0, 7, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_in_file(root_page_id, &ui, TEST_DATA_RANDOM_FILE, 0, 15, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_in_file(root_page_id, &ui, TEST_DATA_RANDOM_FILE, 0, 31, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_in_file(root_page_id, &di, TEST_DATA_FILE, 179, 0, 1, 0, 1, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	res = update_in_file(root_page_id, &ri, TEST_DATA_FILE, 4, 4, 256, 0, 0, &bpttd, dam_p, pmm_p);

	printf("reads using updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* DELETE USING UPDATE FUNCTIONALITY */
#define TEST_DELETE_USING_UPDATE

#ifdef TEST_DELETE_USING_UPDATE
	res = update_in_file(root_page_id, &di, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 1, &bpttd, dam_p, pmm_p);

	printf("updates to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);
#endif

	// delete all
	/* DELETIONS STARTED */

	res = delete_from_file(root_page_id, TEST_DATA_RANDOM_FILE, 0, 0, 256, 0, 1, &bpttd, dam_p, pmm_p);

	printf("deletions to bplus tree completed (%u of %u)\n\n", res.operations_succeeded, res.records_processed);

	/* DELETIONS COMPLETED */



	/* CLEANUP */

	// destroy bplus tree
	destroy_bplus_tree(root_page_id, &bpttd, dam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(dam_p);

	// destroy bplus_tree_tuple_definitions
	deinit_bplus_tree_tuple_definitions(&bpttd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	// delete the record definition
	delete_tuple_def(record_def);

	return 0;
}