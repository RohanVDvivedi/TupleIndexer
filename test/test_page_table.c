#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>

#include<tupleindexer/page_table/page_table.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

#define START_BUCKET_ID 12
#define DIFFR_BUCKET_ID 30 // must be multiple of 2
#define COUNT_BUCKET_ID 100

#define ith_bucket_id(i) ((START_BUCKET_ID) + ((DIFFR_BUCKET_ID) * (i)))
#define i_by_2_th_bucket_id(i) ((START_BUCKET_ID) + (((DIFFR_BUCKET_ID)/2) * (i)))

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

int vaccum_in_page_table(uint64_t root_page_id, uint64_t vaccum_bucket_id, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, pttd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	int success = perform_vaccum_page_table_range_locker(ptrl_p, vaccum_bucket_id, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_page_table_range_locker(ptrl_p, NULL, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	return success;
}

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// construct tuple definitions for page_table
	page_table_tuple_defs pttd;
	init_page_table_tuple_definitions(&pttd, &(pam_p->pas));

	// print the generated page_table tuple defs
	print_page_table_tuple_definitions(&pttd);

	// create a page_table and get its root
	uint64_t root_page_id = get_new_page_table(&pttd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* SETUP COMPLETED */
	printf("\n");

	int vaccum_needed;
	uint64_t vaccum_bucket_id;

	/* TESTS STARTED */

	// print the constructed page table
	print_page_table(root_page_id, 0, &pttd, pam_p, transaction_id, &abort_error);

	page_table_range_locker* ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &pttd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("setting every 500\n\n");
	for(uint64_t i = 0; i < COUNT_BUCKET_ID; i++)
		set_in_page_table(ptrl_p, ith_bucket_id(i), ith_bucket_id(i), transaction_id, &abort_error);
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(vaccum_needed)
		vaccum_in_page_table(root_page_id, vaccum_bucket_id, &pttd, pam_p, pmm_p);

	// print the constructed page table
	print_page_table(root_page_id, 1, &pttd, pam_p, transaction_id, &abort_error);

	ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &pttd, pam_p, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("printing every 250\n\n");
	for(uint64_t i = 0; i < COUNT_BUCKET_ID * 2; i++)
		printf("%"PRIu64 " -> %"PRIu64"\n", i_by_2_th_bucket_id(i), get_from_page_table(ptrl_p, i_by_2_th_bucket_id(i), transaction_id, &abort_error));
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(vaccum_needed)
		vaccum_in_page_table(root_page_id, vaccum_bucket_id, &pttd, pam_p, pmm_p);

	ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &pttd, pam_p, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("getting all by find forward\n\n");
	uint64_t bucket_id;
	uint64_t page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, MIN, transaction_id, &abort_error);
	while(page_id != pam_p->pas.NULL_PAGE_ID)
	{
		printf("%"PRIu64" -> %"PRIu64"\n", bucket_id, page_id);
		page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, GREATER_THAN, transaction_id, &abort_error);
	}
	printf("\n\n");

	printf("getting all by find reverse\n\n");
	page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, MAX, transaction_id, &abort_error);
	while(page_id != pam_p->pas.NULL_PAGE_ID)
	{
		printf("%"PRIu64" -> %"PRIu64"\n", bucket_id, page_id);
		page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, LESSER_THAN, transaction_id, &abort_error);
	}
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(vaccum_needed)
		vaccum_in_page_table(root_page_id, vaccum_bucket_id, &pttd, pam_p, pmm_p);

	ptrl_p = get_new_page_table_range_locker(root_page_id, WHOLE_BUCKET_RANGE, &pttd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("setting every 500 to NULL_PAGE_ID\n\n");
	for(uint64_t i = 0; i < COUNT_BUCKET_ID; i++)
		set_in_page_table(ptrl_p, ith_bucket_id(i), pam_p->pas.NULL_PAGE_ID, transaction_id, &abort_error);
	printf("\n\n");

	delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(vaccum_needed)
		vaccum_in_page_table(root_page_id, vaccum_bucket_id, &pttd, pam_p, pmm_p);

	// print the constructed page table
	print_page_table(root_page_id, 1, &pttd, pam_p, transaction_id, &abort_error);

	/* TESTS ENDED */

	/* CLEANUP */

	// destroy page_table
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