#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<worm.h>

#include<unWALed_in_memory_data_store.h>
#include<unWALed_page_modification_methods.h>

#define BUFFER_APPEND_COUNT 10

#define BUFFER_APPEND_SIZE 100
//#define BUFFER_APPEND_SIZE 244 // equal to wha can ft on page
//#define BUFFER_APPEND_SIZE 1000 // much larger than what could fit on page

char buffer[BUFFER_APPEND_SIZE];

void prepare_buffer(uint8_t id)
{
	for(uint32_t i = 0; i < BUFFER_APPEND_SIZE; i++)
	{
		if((i%2) == 0)
			buffer[i] = id;
		else
			buffer[i] = (i/2);
	}
}

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// construct tuple definitions for worm
	worm_tuple_defs wtd;
	init_worm_tuple_definitions(&wtd, &(pam_p->pas));

	// print the generated worm tuple defs
	print_worm_tuple_definitions(&wtd);

	// create a worm and get its head
	uint64_t head_page_id = get_new_worm(1, pam_p->pas.NULL_PAGE_ID, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	uint32_t id = 0;

	// APPEND WORM
	{
		worm_append_iterator* wai_p = get_new_worm_append_iterator(head_page_id, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		int t = BUFFER_APPEND_COUNT;
		while(t > 0)
		{
			prepare_buffer(id++);
			uint32_t bytes_appended = append_to_worm(wai_p, buffer, (id == 1 && BUFFER_APPEND_SIZE == 244) ? 230 : BUFFER_APPEND_SIZE, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			printf("bytes_appended = %"PRIu32"\n", bytes_appended);
			t--;
		}

		delete_worm_append_iterator(wai_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	/* PRINT WORM */
	print_worm(head_page_id, &wtd, pam_p, transaction_id, &abort_error);

	// APPEND WORM
	{
		worm_append_iterator* wai_p = get_new_worm_append_iterator(head_page_id, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		int t = BUFFER_APPEND_COUNT;
		while(t > 0)
		{
			prepare_buffer(id++);
			uint32_t bytes_appended = append_to_worm(wai_p, buffer, (id == 0 && BUFFER_APPEND_SIZE == 244) ? 230 : BUFFER_APPEND_SIZE, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			printf("bytes_appended = %"PRIu32"\n", bytes_appended);
			t--;
		}

		delete_worm_append_iterator(wai_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	/* PRINT WORM */
	print_worm(head_page_id, &wtd, pam_p, transaction_id, &abort_error);

	/* CLEANUP */

	// destroy bplus tree
	decrement_reference_counter_for_worm(head_page_id, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy worm_tuple_definitions
	deinit_worm_tuple_definitions(&wtd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	return 0;
}