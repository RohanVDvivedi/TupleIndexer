#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tupleindexer/worm/worm.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

#define BUFFER_APPEND_COUNT 10

#define BUFFER_APPEND_SIZE 100
//#define BUFFER_APPEND_SIZE 244 // equal to what can fit on page
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

typedef struct worm_position worm_position;
struct worm_position
{
	uint64_t page_id;
	uint32_t blob_index;
};

uint32_t positions_size = 0;
worm_position positions[BUFFER_APPEND_COUNT * 10];

void print_worm_from_ramdom(worm_position pos, uint32_t buffer_size, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p)
{
	worm_read_iterator* wri_p = get_new_worm_read_iterator2(pos.page_id, pos.blob_index, wtd_p, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("WORM DEPENDENT_ROOT : %"PRIu64"\n", get_dependent_root_page_id_worm_read_iterator(wri_p));

	void* buffer = malloc(buffer_size);
	uint32_t bytes_read = 0;

	printf("WORM PRINT AS BYTES :: \n");
	while((bytes_read = read_from_worm(wri_p, buffer, buffer_size, transaction_id, &abort_error)) > 0 && (!abort_error))
	{
		for(uint32_t i = 0; i < bytes_read; i++)
			printf("0x%02x ", *(uint8_t*)(buffer + i));
		printf("\n");
	}

	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	free(buffer);

	delete_worm_read_iterator(wri_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void print_worm_as_bytes(uint64_t head_page_id, uint32_t buffer_size, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p)
{
	worm_read_iterator* wri_p = get_new_worm_read_iterator(head_page_id, wtd_p, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("WORM DEPENDENT_ROOT : %"PRIu64"\n", get_dependent_root_page_id_worm_read_iterator(wri_p));

	void* buffer = malloc(buffer_size);
	uint32_t bytes_read = 0;

	printf("WORM PRINT AS BYTES :: \n");
	while((bytes_read = read_from_worm(wri_p, buffer, buffer_size, transaction_id, &abort_error)) > 0 && (!abort_error))
	{
		for(uint32_t i = 0; i < bytes_read; i++)
			printf("0x%02x ", *(uint8_t*)(buffer + i));
		printf("\n");
	}

	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	free(buffer);

	delete_worm_read_iterator(wri_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void print_worm_as_bytes2(uint64_t head_page_id, uint32_t buffer_size, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p)
{
	worm_read_iterator* wri_p = get_new_worm_read_iterator(head_page_id, wtd_p, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("WORM DEPENDENT_ROOT : %"PRIu64"\n", get_dependent_root_page_id_worm_read_iterator(wri_p));

	const void* buffer = NULL;
	uint32_t bytes_read = 0;

	printf("WORM PRINT AS BYTES :: \n");
	while(1)
	{
		buffer = peek_in_worm(wri_p, &bytes_read, transaction_id, &abort_error);
		if(bytes_read == 0)
			break;

		bytes_read = min(bytes_read, buffer_size);

		if(abort_error)
			break;

		for(uint32_t i = 0; i < bytes_read; i++)
			printf("0x%02x ", *(uint8_t*)(buffer + i));
		printf("\n");

		read_from_worm(wri_p, NULL, bytes_read, transaction_id, &abort_error);
		if(abort_error)
			break;
	}

	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_worm_read_iterator(wri_p, transaction_id, &abort_error);
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

	/* PRINT WORM */
	print_worm(head_page_id, &wtd, pam_p, transaction_id, &abort_error);

	print_worm_as_bytes(head_page_id, 8, &wtd, pam_p);

	print_worm_as_bytes2(head_page_id, 8, &wtd, pam_p);

	/* make some initial get set calls */
	increment_reference_counter_for_worm(head_page_id, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	set_dependent_root_page_id_for_worm(head_page_id, 12, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	printf("setting dependent_root_page_id = %"PRIu64"\n", get_dependent_root_page_id_for_worm(head_page_id, &wtd, pam_p, transaction_id, &abort_error));

	/* PRINT WORM */
	print_worm(head_page_id, &wtd, pam_p, transaction_id, &abort_error);

	print_worm_as_bytes(head_page_id, 8, &wtd, pam_p);

	print_worm_as_bytes2(head_page_id, 8, &wtd, pam_p);

	uint32_t id = 0;

	// APPEND WORM
	{
		worm_append_iterator* wai_p = get_new_worm_append_iterator(head_page_id, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		uint64_t dependent_root_page_id = get_dependent_root_page_id_worm_append_iterator(wai_p);
		set_dependent_root_page_id_worm_append_iterator(wai_p, dependent_root_page_id + 1, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		increment_reference_counter_worm_append_iterator(wai_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		int t = BUFFER_APPEND_COUNT;
		while(t > 0)
		{
			prepare_buffer(id++);
			uint32_t bytes_appended = append_to_worm(wai_p, buffer, (id == 1 && BUFFER_APPEND_SIZE == 244) ? 230 : BUFFER_APPEND_SIZE, NULL, NULL, transaction_id, &abort_error);
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

	print_worm_as_bytes(head_page_id, 600, &wtd, pam_p);

	print_worm_as_bytes2(head_page_id, 600, &wtd, pam_p);

	// APPEND WORM
	{
		worm_append_iterator* wai_p = get_new_worm_append_iterator(head_page_id, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		uint64_t dependent_root_page_id = get_dependent_root_page_id_worm_append_iterator(wai_p);
		set_dependent_root_page_id_worm_append_iterator(wai_p, dependent_root_page_id + 1, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		increment_reference_counter_worm_append_iterator(wai_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		int t = BUFFER_APPEND_COUNT;
		while(t > 0)
		{
			prepare_buffer(id++);
			uint32_t bytes_appended = append_to_worm(wai_p, buffer, (id == 0 && BUFFER_APPEND_SIZE == 244) ? 230 : BUFFER_APPEND_SIZE, &(positions[positions_size].page_id), &(positions[positions_size].blob_index), transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			printf("bytes_appended = %"PRIu32"\n", bytes_appended);
			positions_size++;
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

	print_worm_as_bytes(head_page_id, 16, &wtd, pam_p);

	print_worm_as_bytes2(head_page_id, 16, &wtd, pam_p);

	for(uint32_t i = 0; i < positions_size; i++)
	{
		printf("\n\n printing from = %"PRIu64", %"PRIu32"\n\n", positions[i].page_id, positions[i].blob_index);
		print_worm_from_ramdom(positions[i], 16, &wtd, pam_p);
	}

	/* CLEANUP */

	// destroy bplus tree
	int vaccum_needed = 0;
	uint64_t dependent_root_page_id = 0;

	decrement_reference_counter_for_worm(head_page_id, &dependent_root_page_id, &vaccum_needed, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	printf("decrementing reference counter => vaccum = %d %"PRIu64"\n", vaccum_needed, dependent_root_page_id);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	decrement_reference_counter_for_worm(head_page_id, &dependent_root_page_id, &vaccum_needed, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	printf("decrementing reference counter => vaccum = %d %"PRIu64"\n", vaccum_needed, dependent_root_page_id);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	decrement_reference_counter_for_worm(head_page_id, &dependent_root_page_id, &vaccum_needed, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	printf("decrementing reference counter => vaccum = %d %"PRIu64"\n", vaccum_needed, dependent_root_page_id);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	decrement_reference_counter_for_worm(head_page_id, &dependent_root_page_id, &vaccum_needed, &wtd, pam_p, pmm_p, transaction_id, &abort_error);
	printf("decrementing reference counter => vaccum = %d %"PRIu64"\n", vaccum_needed, dependent_root_page_id);
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