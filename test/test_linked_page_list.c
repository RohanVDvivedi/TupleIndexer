#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuple.h>
#include<tuple_def.h>

#include<linked_page_list.h>
#include<linked_page_list_tuple_definitions.h>
#include<page_access_methods.h>
#include<unWALed_in_memory_data_store.h>
#include<unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256
#define SYSTEM_HEADER_SIZE   3

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	tuple_def* def = get_new_tuple_def("students", 2, PAGE_SIZE);

	insert_element_def(def, "index", INT, 4, 0, NULL_USER_VALUE);
	insert_element_def(def, "name", VAR_STRING, 1, 0, NULL_USER_VALUE);

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

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE, .system_header_size = SYSTEM_HEADER_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// construct tuple definitions for linked_page_list
	linked_page_list_tuple_defs lpltd;
	init_linked_page_list_tuple_definitions(&lpltd, &(pam_p->pas), record_def);

	// print the generated linked page list tuple defs
	print_linked_page_list_tuple_definitions(&lpltd);

	// create a linked_page_list and get its head
	uint64_t head_page_id = get_new_linked_page_list(&lpltd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* PRINT EMPTY LINKED_PAGE_LIST */

	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	/* CREATE A SAMPLE LINKED_PAGE_LIST_ITERATOR AND USE IT */

	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, WRITE_LOCK, &lpltd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("is_writable = %d\n", is_writable_linked_page_list_iterator(lpli_p));
	printf("state = %d\n", get_state_for_linked_page_list(lpli_p));
	printf("is_empty = %d\n", is_empty_linked_page_list(lpli_p));
	printf("is_at_head_page = %d\n", is_at_head_page_linked_page_list_iterator(lpli_p));
	printf("is_at_tail_page = %d\n", is_at_tail_page_linked_page_list_iterator(lpli_p));
	printf("is_as_head_tuple = %d\n", is_at_head_tuple_linked_page_list_iterator(lpli_p));
	printf("is_at_tail_tuple = %d\n", is_at_tail_tuple_linked_page_list_iterator(lpli_p));

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* CLEANUP */

	// destroy linked_page_list
	destroy_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy linked_page_list_tuple_definitions
	deinit_linked_page_list_tuple_definitions(&lpltd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	// delete the record definition
	delete_tuple_def(record_def);

	return 0;
}