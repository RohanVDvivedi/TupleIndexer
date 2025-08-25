#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/heap_page/heap_page.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          128

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

typedef struct fix_entry fix_entry;
struct fix_entry
{
	uint32_t unused_space;
	uint64_t page_id;
};

uint32_t fix_entries_size = 0;
fix_entry fix_entries[2048];

void fix_notify(void* context, uint32_t unused_space, uint64_t page_id)
{
	fix_entries[fix_entries_size++] = (fix_entry){unused_space, page_id};
}

#define FIX_NOTIFIER &((heap_table_notifier){NULL, fix_notify})

int insertion_index = 0;

// last of the names should be NULL
void insert_tuples_to_heap_table(uint64_t root_page_id, char** names, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	int is_tracked = 0;
	uint32_t unused_space_in_entry = 0;
	persistent_page heap_page = get_NULL_persistent_page(pam_p);
	uint32_t possible_insertion_index = 0;

	for(char** name = names; (*name) != NULL; name++)
	{
		// build the tuple that we need to insert
		char tuple[256];
		initialize_tuple(httd_p->record_def, tuple, insertion_index++, (*name));

		// calculate required space for this tuple
		uint32_t required_space = get_space_to_be_occupied_by_tuple_on_persistent_page(httd_p->pas_p->page_size, &(httd_p->record_def->size_def), tuple);

		// if some heap page is currently locked, then ensure it has enough unused_space, else release lock on it
		if(!is_persistent_page_NULL(&heap_page, pam_p))
		{
			if(required_space > get_unused_space_on_heap_page(&heap_page, httd_p->pas_p, httd_p->record_def))
			{
				if(is_tracked == 0)
				{
					track_unused_space_in_heap_table(root_page_id, &heap_page, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
					if(abort_error)
					{
						printf("ABORTED\n");
						exit(-1);
					}
				}
				else
				{
					fix_notify(NULL, unused_space_in_entry, heap_page.page_id);
				}
				release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, NONE_OPTION, &abort_error);
				if(abort_error)
				{
					printf("ABORTED\n");
					exit(-1);
				}
			}
		}

		// if the heap_page is NULL then find one to insert this tuple into
		if(is_persistent_page_NULL(&heap_page, pam_p))
		{
			// we are possibly inserting into a new page, so we can possibly know the index to insert into it
			possible_insertion_index = 0;

			heap_page = find_heap_page_with_enough_unused_space_from_heap_table(root_page_id, required_space, &unused_space_in_entry, FIX_NOTIFIER, httd_p, pam_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(is_persistent_page_NULL(&heap_page, pam_p))
			{
				heap_page = get_new_heap_page_with_write_lock(httd_p->pas_p, httd_p->record_def, pam_p, pmm_p, transaction_id, &abort_error);
				if(abort_error)
				{
					printf("ABORTED\n");
					exit(-1);
				}
				is_tracked = 0;
			}
			else
				is_tracked = 1;
		}

		// insert into the heap_page
		uint32_t index = insert_in_heap_page(&heap_page, tuple, &possible_insertion_index, httd_p->record_def, httd_p->pas_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		printf("(%"PRIu64", %"PRIu32") -> ", heap_page.page_id, index);
		print_tuple(tuple, httd_p->record_def);
		printf(" (inserted)\n");
	}

	// if some heap page is currently locked, then ensure it has enough unused_space, else release lock on it
	if(!is_persistent_page_NULL(&heap_page, pam_p))
	{
		if(is_tracked == 0)
		{
			track_unused_space_in_heap_table(root_page_id, &heap_page, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
		else
		{
			fix_notify(NULL, unused_space_in_entry, heap_page.page_id);
		}
		release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, NONE_OPTION, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}
}

void fix_all_entries(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	for(uint32_t i = 0; i < fix_entries_size; i++)
	{
		fix_unused_space_in_heap_table(root_page_id, fix_entries[i].unused_space, fix_entries[i].page_id, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}
	fix_entries_size = 0;
}

void delete_tuples_from_heap_table(uint64_t root_page_id, char** names, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	heap_table_iterator* hti_p = get_new_heap_table_iterator(root_page_id, 0, 0, httd_p, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	while(1)
	{
		int entry_needs_fixing = 0;
		uint32_t unused_space_in_entry;
		persistent_page heap_page = lock_and_get_curr_heap_page_heap_table_iterator(hti_p, 1, &unused_space_in_entry, &entry_needs_fixing, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		if(is_persistent_page_NULL(&heap_page, pam_p))
			break;

		if(entry_needs_fixing)
			fix_notify(NULL, unused_space_in_entry, heap_page.page_id);

		for(uint32_t i = 0; i < get_tuple_count_on_persistent_page(&heap_page, httd_p->pas_p->page_size, &(httd_p->record_def->size_def)); i++)
		{
			const void* tuple = get_nth_tuple_on_persistent_page(&heap_page, httd_p->pas_p->page_size, &(httd_p->record_def->size_def), i);
			if(tuple == NULL)
				continue;
			user_value uval;
			int res = get_value_from_element_from_tuple(&uval, httd_p->record_def, STATIC_POSITION(1), tuple);
			if(!res || is_user_value_NULL(&uval))
				continue;

			int match = 0;
			for(char** name = names; (*name) != NULL && match == 0; name++)
			{
				user_value uval2 = {.string_value = (*name), .string_size = strlen((*name))};
				if(compare_user_value2(&uval, &uval2, httd_p->record_def->type_info->containees[1].al.type_info) == 0)
					match = 1;
			}

			if(match)
			{
				delete_from_heap_page(&heap_page, i, httd_p->record_def, httd_p->pas_p, pmm_p, transaction_id, &abort_error);
				if(abort_error)
				{
					printf("ABORTED\n");
					exit(-1);
				}
			}
		}

		release_lock_on_persistent_page(pam_p, transaction_id, &heap_page, NONE_OPTION, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		next_heap_table_iterator(hti_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}

	delete_heap_table_iterator(hti_p, transaction_id, &abort_error);
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

	// allocate record tuple definition and initialize it
	tuple_def* record_def = get_tuple_definition();

	// construct tuple definitions for heap_table
	heap_table_tuple_defs httd;
	init_heap_table_tuple_definitions(&httd, &(pam_p->pas), record_def);

	// print the generated heap table tuple defs
	print_heap_table_tuple_definitions(&httd);

	// create a heap_table and get its head
	uint64_t root_page_id = get_new_heap_table(&httd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	/* PRINT EMPTY HEAP_TABLE */

	print_heap_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);

	/* TESTS */

	insert_tuples_to_heap_table(root_page_id, (char*[]){"Rohan Vipulkumar Dvivedi", "Rupa Vipulkumar Dvivedi", "Shirdiwala Saibaba, Jako rakhe saiya maar sake na koi", "Vipulkumar Bhanuprasad Dvivedi", "Devashree Manan Joshi, Vipulkumar Dvivedi", NULL}, &httd, pam_p, pmm_p);

	print_heap_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);

	fix_all_entries(root_page_id, &httd, pam_p, pmm_p);

	print_heap_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);

	delete_tuples_from_heap_table(root_page_id, (char*[]){"Shirdiwala Saibaba, Jako rakhe saiya maar sake na koi", "Devashree Manan Joshi, Vipulkumar Dvivedi", NULL}, &httd, pam_p, pmm_p);

	print_heap_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);

	/* CLEANUP */

	// destroy heap_table
	destroy_heap_table(root_page_id, &httd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy heap_table_tuple_definitions
	deinit_heap_table_tuple_definitions(&httd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	// delete the record definition

	return 0;
}