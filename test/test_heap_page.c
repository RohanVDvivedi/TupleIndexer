#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

tuple_def tuple_definition;
char tuple_type_info_memory[sizeof_tuple_data_type_info(2)];
data_type_info* tuple_type_info = (data_type_info*)tuple_type_info_memory;
data_type_info c1_type_info;

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	initialize_tuple_data_type_info(tuple_type_info, "students", 1, PAGE_SIZE, 2);

	strcpy(tuple_type_info->containees[0].field_name, "index");
	tuple_type_info->containees[0].al.type_info = INT_NULLABLE[4];

	c1_type_info = get_variable_length_string_type("", 256);
	strcpy(tuple_type_info->containees[1].field_name, "name");
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

void initialize_tuple(const tuple_def* def, char* tuple, int index, const char* name)
{
	init_tuple(def, tuple);

	set_element_in_tuple(def, STATIC_POSITION(0), tuple, &((user_value){.int_value = index}), UINT32_MAX);
	set_element_in_tuple(def, STATIC_POSITION(1), tuple, &((user_value){.string_value = name, .string_size = strlen(name)}), UINT32_MAX);
}

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// get a new persistent page to work with
	persistent_page heap_page = get_new_heap_page_with_write_lock(&(pam_p->pas), record_def, pam_p, pmm_p, transaction_id, &abort_error);

	/* TESTS STARTED */

	print_heap_page(&heap_page, &(pam_p->pas), record_def);

	/* CLEANUP */

	// destroy bplus tree
	release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, FREE_PAGE, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	return 0;
}