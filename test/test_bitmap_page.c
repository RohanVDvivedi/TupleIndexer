#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/bitmap_page/bitmap_page.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

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

	// allocate record tuple definition and initialize it
	uint64_t bit_field_count;
	tuple_def* bit_fields_def = get_tuple_definition_for_bitmap_page(&(pam_p->pas), 13, &bit_field_count);
	print_tuple_def(bit_fields_def);
	printf("\n\n");

	// get a new persistent page to work with
	persistent_page bitmap_page = get_new_bitmap_page_with_write_lock(&(pam_p->pas), bit_fields_def, pam_p, pmm_p, transaction_id, &abort_error);

	/* TESTS STARTED */

	print_bitmap_page(&bitmap_page, &(pam_p->pas), bit_fields_def);

	for(uint32_t i = 0; i < bit_field_count; i++)
	{
		set_bit_field_on_bitmap_page(&bitmap_page, i, i + 100, &(pam_p->pas), bit_fields_def, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	printf("\n\n");
	for(uint32_t i = 0; i < bit_field_count; i++)
	{
		printf("%"PRIu32" -> %"PRIu64"\n", i, get_bit_field_on_bitmap_page(&bitmap_page, i, &(pam_p->pas), bit_fields_def));
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}
	printf("\n\n");

	for(uint32_t i = bit_field_count - 1; i != -1; i--)
	{
		set_bit_field_on_bitmap_page(&bitmap_page, i, i + 200, &(pam_p->pas), bit_fields_def, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	printf("\n\n");
	for(uint32_t i = 0; i < bit_field_count; i++)
	{
		printf("%"PRIu32" -> %"PRIu64"\n", i, get_bit_field_on_bitmap_page(&bitmap_page, i, &(pam_p->pas), bit_fields_def));
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}
	printf("\n\n");

	print_bitmap_page(&bitmap_page, &(pam_p->pas), bit_fields_def);

	/* CLEANUP */

	// destroy bplus tree
	release_lock_on_persistent_page(pam_p, transaction_id, &bitmap_page, FREE_PAGE, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	destroy_non_static_type_info_recursively(bit_fields_def->type_info);
	free(bit_fields_def);

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	return 0;
}