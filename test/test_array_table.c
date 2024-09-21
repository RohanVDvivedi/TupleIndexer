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
#define SYSTEM_HEADER_SIZE   3

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

uint32_t access_counter = 0;

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	tuple_def* def = get_new_tuple_def("entry", 2, PAGE_SIZE);

	insert_element_def(def, "access_index", UINT, 4, 1, ZERO_USER_VALUE);
	insert_element_def(def, "content", STRING, 60, 1, EMPTY_USER_VALUE);

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
			char record[64];
			init_tuple(attd_p->record_def, record);
			set_element_in_tuple(attd_p->record_def, 0, record, &(user_value){.uint_value = access_counter});
			set_element_in_tuple(attd_p->record_def, 1, record, &(user_value){.data = datas[i], .data_size = strlen(datas[i])});

			success += set_in_array_table(atrl_p, bucket_ids[i], record, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
	}

	delete_array_table_range_locker(atrl_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

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
		char memory[64];
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

	delete_array_table_range_locker(atrl_p, transaction_id, &abort_error);
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
		char memory[64];
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
		char memory[64];
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

	delete_array_table_range_locker(atrl_p, transaction_id, &abort_error);
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
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE, .system_header_size = SYSTEM_HEADER_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// construct tuple definitions for page_table
	array_table_tuple_defs attd;
	init_array_table_tuple_definitions(&attd, &(pam_p->pas), record_def);

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
	delete_tuple_def(record_def);

	return 0;
}