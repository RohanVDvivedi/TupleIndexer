#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>

#include<page_table.h>
#include<page_table_tuple_definitions.h>
#include<page_access_methods.h>
#include<unWALed_in_memory_data_store.h>
#include<unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256
#define SYSTEM_HEADER_SIZE   3

#define START_BUCKET_ID 12
#define DIFFR_BUCKET_ID 30 // must be multiple of 2
#define COUNT_BUCKET_ID 100

#define ith_bucket_id(i) (START_BUCKET_ID + (DIFFR_BUCKET_ID * i))
#define i_by_2_th_bucket_id(i) (START_BUCKET_ID + ((DIFFR_BUCKET_ID/2) * i))

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE, .system_header_size = SYSTEM_HEADER_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// construct tuple definitions for bplus_tree
	page_table_tuple_defs pttd;
	init_page_table_tuple_definitions(&pttd, &(pam_p->pas));

	// print the generated bplus tree tuple defs
	print_page_table_tuple_definitions(&pttd);

	// create a bplus tree and get its root
	uint64_t root_page_id = get_new_page_table(&pttd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* SETUP COMPLETED */
	printf("\n");

	/* TESTS STARTED */

	// print the constructed page table
	print_page_table(root_page_id, 0, &pttd, pam_p, transaction_id, &abort_error);

	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, WRITE_LOCK, &pttd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("setting every 500\n\n");
	for(uint64_t i = 0; i < COUNT_BUCKET_ID; i++)
		set_in_page_table(ptrl_p, ith_bucket_id(i), ith_bucket_id(i), pmm_p, transaction_id, &abort_error);
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// print the constructed page table
	print_page_table(root_page_id, 1, &pttd, pam_p, transaction_id, &abort_error);

	ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, READ_LOCK, &pttd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("printing every 250\n\n");
	for(uint64_t i = 0; i < COUNT_BUCKET_ID * 2; i++)
		printf("%"PRIu64 " -> %"PRIu64"\n", i_by_2_th_bucket_id(i), get_from_page_table(ptrl_p, i_by_2_th_bucket_id(i), transaction_id, &abort_error));
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, READ_LOCK, &pttd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("getting all by find forward\n\n");
	uint64_t bucket_id = 0;
	uint64_t page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, GREATER_THAN_EQUALS, transaction_id, &abort_error);
	while(page_id != pam_p->pas.NULL_PAGE_ID)
	{
		printf("%"PRIu64" -> %"PRIu64"\n", bucket_id, page_id);
		page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, GREATER_THAN, transaction_id, &abort_error);
	}
	printf("\n\n");

	printf("getting all by find reverse\n\n");
	bucket_id = UINT64_MAX;
	page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, LESSER_THAN_EQUALS, transaction_id, &abort_error);
	while(page_id != pam_p->pas.NULL_PAGE_ID)
	{
		printf("%"PRIu64" -> %"PRIu64"\n", bucket_id, page_id);
		page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, LESSER_THAN, transaction_id, &abort_error);
	}
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_PAGE_TABLE_BUCKET_RANGE, WRITE_LOCK, &pttd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("setting every 500 to NULL_PAGE_ID\n\n");
	for(uint64_t i = 0; i < COUNT_BUCKET_ID; i++)
		set_in_page_table(ptrl_p, ith_bucket_id(i), pam_p->pas.NULL_PAGE_ID, pmm_p, transaction_id, &abort_error);
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// print the constructed page table
	print_page_table(root_page_id, 1, &pttd, pam_p, transaction_id, &abort_error);

	// print the constructed page table
	//print_page_table(root_page_id, 1, &pttd, pam_p, transaction_id, &abort_error);

	/* TESTS ENDED */

	/* CLEANUP */

	// destroy bplus tree
	destroy_page_table(root_page_id, &pttd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy page_table_tuple_definitions
	deinit_page_table_tuple_definitions(&pttd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	return 0;
}