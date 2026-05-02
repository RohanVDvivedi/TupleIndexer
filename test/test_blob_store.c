#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/blob_store/blob_store.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

typedef struct blob_pointer blob_pointer;
struct blob_pointer
{
	uint64_t head_page_id;
	uint32_t head_tuple_index;

	uint64_t tail_page_id;
	uint32_t tail_tuple_index;
};

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// construct tuple definitions for blob_store
	blob_store_tuple_defs bstd;
	init_blob_store_tuple_definitions(&bstd, &(pam_p->pas));

	// print the generated blob_store tuple defs
	print_blob_store_tuple_definitions(&bstd);

	// create a blob_store
	uint64_t root_page_id = get_new_blob_store(&bstd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// print whole of blob store
	print_blob_store(root_page_id, &bstd, pam_p, transaction_id, &abort_error);

	/* CLEANUP */
	// destroy blob_store
	destroy_blob_store(root_page_id, &bstd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy blob_store_tuple_definitions
	deinit_blob_store_tuple_definitions(&bstd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	return 0;
}