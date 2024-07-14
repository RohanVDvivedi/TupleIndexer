#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>

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

void update_in_array_table(uint64_t root_page_id, bucket_range lock_range, uint64_t ops, uint64_t* bucket_ids, char** datas, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{

}

void print_from_array_table(uint64_t root_page_id, bucket_range lock_range, uint64_t ops, uint64_t* bucket_ids, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p)
{

}

void print_all_from_array_table(uint64_t root_page_id, bucket_range lock_range, uint64_t ops, int backward, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p)
{

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

	

	// print the constructed page table
	print_array_table(root_page_id, 1, &attd, pam_p, transaction_id, &abort_error);

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

	return 0;
}