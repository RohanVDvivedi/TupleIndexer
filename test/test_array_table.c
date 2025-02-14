#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuple.h>
#include<tuple_def.h>

#include<array_table.h>

#include<unWALed_in_memory_data_store.h>
#include<unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

uint32_t access_counter = 0;

tuple_def tuple_definition;
char tuple_type_info_memory[sizeof_tuple_data_type_info(2)];
data_type_info* tuple_type_info = (data_type_info*)tuple_type_info_memory;
data_type_info c1_type_info;

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	initialize_tuple_data_type_info(tuple_type_info, "entry", 1, PAGE_SIZE, 2);

	strcpy(tuple_type_info->containees[0].field_name, "access_index");
	tuple_type_info->containees[0].al.type_info = UINT_NON_NULLABLE[4];

	c1_type_info = get_fixed_length_string_type("", 60, 0);
	strcpy(tuple_type_info->containees[1].field_name, "content");
	tuple_type_info->containees[1].al.type_info = &c1_type_info;

	if(!initialize_tuple_def(&tuple_definition, tuple_type_info))
	{
		printf("failed finalizing tuple definition\n");
		exit(-1);
	}

	print_tuple_def(&tuple_definition);
	printf("\n\n");

	return &tuple_definition;
}

int vaccum_in_array_table(uint64_t root_page_id, uint64_t vaccum_bucket_id, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	array_table_range_locker* atrl_p = get_new_array_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, attd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	int success = perform_vaccum_array_table_range_locker(atrl_p, vaccum_bucket_id, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_array_table_range_locker(atrl_p, NULL, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	return success;
}

int update_in_array_table(uint64_t root_page_id, bucket_range lock_range, uint32_t ops, uint64_t* bucket_ids, char** datas, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	array_table_range_locker* atrl_p = get_new_array_table_range_locker(root_page_id, lock_range, attd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	int success = 0;

	for(uint32_t i = 0; i < ops; i++, access_counter++)
	{
		if(datas[i] == NULL)
		{
			success += set_in_array_table(atrl_p, bucket_ids[i], NULL, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
		else
		{
			char record[PAGE_SIZE];
			init_tuple(attd_p->record_def, record);
			set_element_in_tuple(attd_p->record_def, STATIC_POSITION(0), record, &(user_value){.uint_value = access_counter}, UINT32_MAX);
			set_element_in_tuple(attd_p->record_def, STATIC_POSITION(1), record, &(user_value){.string_value = datas[i], .string_size = strlen(datas[i])}, UINT32_MAX);

			success += set_in_array_table(atrl_p, bucket_ids[i], record, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
	}

	int vaccum_needed;
	uint64_t vaccum_bucket_id;
	delete_array_table_range_locker(atrl_p, &vaccum_bucket_id, &vaccum_needed, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(vaccum_needed)
		vaccum_in_array_table(root_page_id, vaccum_bucket_id, attd_p, pam_p, pmm_p);

	return success;
}

void print_from_array_table(uint64_t root_page_id, bucket_range lock_range, uint32_t ops, uint64_t* bucket_ids, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p)
{
	array_table_range_locker* atrl_p = get_new_array_table_range_locker(root_page_id, lock_range, attd_p, pam_p, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	for(uint32_t i = 0; i < ops; i++)
	{
		char memory[PAGE_SIZE];
		const void* record = get_from_array_table(atrl_p, bucket_ids[i], memory, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		if(record == NULL)
			printf("%"PRIu64" -> NULL\n", bucket_ids[i]);
		else
		{
			printf("%"PRIu64" -> ", bucket_ids[i]);
			print_tuple(record, attd_p->record_def);
		}
	}

	delete_array_table_range_locker(atrl_p, NULL, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void print_all_from_array_table(uint64_t root_page_id, bucket_range lock_range, int backward, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p)
{
	array_table_range_locker* atrl_p = get_new_array_table_range_locker(root_page_id, lock_range, attd_p, pam_p, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(!backward)
	{
		char memory[PAGE_SIZE];
		uint64_t bucket_id;
		const void* record = find_non_NULL_entry_in_array_table(atrl_p, &bucket_id, memory, MIN, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		while(record)
		{
			printf("%"PRIu64" -> ", bucket_id);
			print_tuple(record, attd_p->record_def);

			record = find_non_NULL_entry_in_array_table(atrl_p, &bucket_id, memory, GREATER_THAN, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
	}
	else
	{
		char memory[PAGE_SIZE];
		uint64_t bucket_id;
		const void* record = find_non_NULL_entry_in_array_table(atrl_p, &bucket_id, memory, MAX, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		while(record)
		{
			printf("%"PRIu64" -> ", bucket_id);
			print_tuple(record, attd_p->record_def);

			record = find_non_NULL_entry_in_array_table(atrl_p, &bucket_id, memory, LESSER_THAN, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
	}

	delete_array_table_range_locker(atrl_p, NULL, NULL, transaction_id, &abort_error);
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

	printf("maximum record size allowable = %"PRIu32"\n\n", get_maximum_array_table_record_size(&(pam_p->pas)));

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// construct tuple definitions for page_table
	array_table_tuple_defs attd;
	if(!init_array_table_tuple_definitions(&attd, &(pam_p->pas), record_def))
	{
		printf("could not initialize the tuple_defs\n");
		exit(-1);
	}

	// print the generated page_table tuple defs
	print_array_table_tuple_definitions(&attd);

	// create a page_table and get its root
	uint64_t root_page_id = get_new_array_table(&attd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* SETUP COMPLETED */
	printf("\n");

	/* TESTS STARTED */

	// print the constructed page table
	print_array_table(root_page_id, 0, &attd, pam_p, transaction_id, &abort_error);

	int updates;

	// INSERTS

	// perform updates
	updates = update_in_array_table(root_page_id, WHOLE_BUCKET_RANGE, 4, ((uint64_t []){0, 2, 3, UINT64_MAX - 5}), ((char* []){"zero", "two", "three", "UINT64_MAX - 5"}), &attd, pam_p, pmm_p);
	printf("updates = %d\n\n", updates);

	// print the constructed page table
	print_array_table(root_page_id, 0, &attd, pam_p, transaction_id, &abort_error);

	// print inserted tuples
	print_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 8, ((uint64_t []){0, 1, 2, 3, 194, UINT64_MAX / 2, UINT64_MAX - 5, UINT64_MAX}), &attd, pam_p);

	print_all_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 0, &attd, pam_p);
	print_all_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 1, &attd, pam_p);




	// UPDATES

	// perform updates
	updates = update_in_array_table(root_page_id, WHOLE_BUCKET_RANGE, 4, ((uint64_t []){194, 2, 3, UINT64_MAX}), ((char* []){"one hundred ninety four", NULL, "threes", "UINT64_MAX"}), &attd, pam_p, pmm_p);
	printf("updates = %d\n\n", updates);

	// print the constructed page table
	print_array_table(root_page_id, 0, &attd, pam_p, transaction_id, &abort_error);

	// print inserted tuples
	print_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 8, ((uint64_t []){0, 1, 2, 3, 194, UINT64_MAX / 2, UINT64_MAX - 5, UINT64_MAX}), &attd, pam_p);

	print_all_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 0, &attd, pam_p);
	print_all_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 1, &attd, pam_p);




	// DELETES

	// perform updates
	updates = update_in_array_table(root_page_id, WHOLE_BUCKET_RANGE, 7, ((uint64_t []){0, 2, 3, 194, 260, UINT64_MAX - 5, UINT64_MAX}), ((char* []){NULL, NULL, NULL, NULL, NULL, NULL, NULL}), &attd, pam_p, pmm_p);
	printf("updates = %d\n\n", updates);

	// print the constructed page table
	print_array_table(root_page_id, 0, &attd, pam_p, transaction_id, &abort_error);

	// print inserted tuples
	print_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 8, ((uint64_t []){0, 1, 2, 3, 194, UINT64_MAX / 2, UINT64_MAX - 5, UINT64_MAX}), &attd, pam_p);

	print_all_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 0, &attd, pam_p);
	print_all_from_array_table(root_page_id, WHOLE_BUCKET_RANGE, 1, &attd, pam_p);




	/* TESTS ENDED */

	/* CLEANUP */

	// destroy page_table
	destroy_array_table(root_page_id, &attd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy page_table_tuple_definitions
	deinit_array_table_tuple_definitions(&attd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	// delete the record definition

	return 0;
}