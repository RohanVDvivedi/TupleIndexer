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
#define PAGE_SIZE          128
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

void initialize_tuple(tuple_def* def, char* tuple, int index, const char* name)
{
	init_tuple(def, tuple);

	set_element_in_tuple(def, 0, tuple, &((user_value){.int_value = index}));
	set_element_in_tuple(def, 1, tuple, &((user_value){.data = name, .data_size = strlen(name)}));
}

int insert_counter = 0;

void push_at_head(uint64_t head_page_id, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	char tuple[64] = {};
	initialize_tuple(lpltd_p->record_def, tuple, insert_counter++, name);
	insert_at_linked_page_list_iterator(lpli_p, tuple, INSERT_BEFORE_LINKED_PAGE_LIST_ITERATOR, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void push_at_tail(uint64_t head_page_id, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	prev_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	char tuple[64] = {};
	initialize_tuple(lpltd_p->record_def, tuple, insert_counter++, name);
	insert_at_linked_page_list_iterator(lpli_p, tuple, INSERT_AFTER_LINKED_PAGE_LIST_ITERATOR, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void print_all_forward(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(is_empty_linked_page_list(lpli_p))
		printf("EMPTY_LINKED_PAGE_LIST");
	else
	{
		do
		{
			print_tuple(get_tuple_linked_page_list_iterator(lpli_p), lpltd_p->record_def);
			next_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
		while(!is_at_head_tuple_linked_page_list_iterator(lpli_p));
	}
	printf("\n");

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void print_all_reverse(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(is_empty_linked_page_list(lpli_p))
		printf("EMPTY_LINKED_PAGE_LIST");
	else
	{
		prev_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		do
		{
			print_tuple(get_tuple_linked_page_list_iterator(lpli_p), lpltd_p->record_def);
			prev_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
		while(!is_at_tail_tuple_linked_page_list_iterator(lpli_p));
	}
	printf("\n");

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void pop_from_head(uint64_t head_page_id, int to_pop, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	while(to_pop > 0 && !is_empty_linked_page_list(lpli_p))
	{
		printf("popping : ");
		print_tuple(get_tuple_linked_page_list_iterator(lpli_p), lpltd_p->record_def);
		remove_from_linked_page_list_iterator(lpli_p, GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		to_pop--;
	}

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void pop_from_tail(uint64_t head_page_id, int to_pop, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	prev_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
	while(to_pop > 0 && !is_empty_linked_page_list(lpli_p))
	{
		printf("popping : ");
		print_tuple(get_tuple_linked_page_list_iterator(lpli_p), lpltd_p->record_def);
		remove_from_linked_page_list_iterator(lpli_p, GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		to_pop--;
	}

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void update_from_head(uint64_t head_page_id, int index, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(is_empty_linked_page_list(lpli_p))
		printf("EMPTY_LINKED_PAGE_LIST");
	else
	{
		while(index)
		{
			next_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			index--;
		}

		printf("updated from : ");
		print_tuple(get_tuple_linked_page_list_iterator(lpli_p), lpltd_p->record_def);

		char tuple[64] = {};
		initialize_tuple(lpltd_p->record_def, tuple, insert_counter++, name);
		update_at_linked_page_list_iterator(lpli_p, tuple, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		printf("updated to : ");
		print_tuple(get_tuple_linked_page_list_iterator(lpli_p), lpltd_p->record_def);
	}
	printf("\n");

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

// at max count rows are updated
int reindex_all(uint64_t head_page_id, int count, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	int updated = 0;

	if(is_empty_linked_page_list(lpli_p))
		printf("EMPTY_LINKED_PAGE_LIST");
	else
	{
		do
		{
			if(count == 0)
				break;

			updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, 0, &((user_value){.int_value = insert_counter++}), transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(name != NULL)
			{
				updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, 1, &((user_value){.data = name, .data_size = strlen(name)}), transaction_id, &abort_error);
				if(abort_error)
				{
					printf("ABORTED\n");
					exit(-1);
				}
			}

			count--;
			if(count == 0)
				break;

			next_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
		while(!is_at_head_tuple_linked_page_list_iterator(lpli_p));
	}
	printf("\n");

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	return updated;
}

// at max count rows are updated
int reindex_all_reverse(uint64_t head_page_id, int count, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	int updated = 0;

	if(is_empty_linked_page_list(lpli_p))
		printf("EMPTY_LINKED_PAGE_LIST");
	else
	{
		prev_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		do
		{
			if(count == 0)
				break;

			updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, 0, &((user_value){.int_value = insert_counter++}), transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(name != NULL)
			{
				updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, 1, &((user_value){.data = name, .data_size = strlen(name)}), transaction_id, &abort_error);
				if(abort_error)
				{
					printf("ABORTED\n");
					exit(-1);
				}
			}

			count--;
			if(count == 0)
				break;

			prev_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
		while(!is_at_tail_tuple_linked_page_list_iterator(lpli_p));
	}
	printf("\n");

	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	return updated;
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

	/* TESTS */

	push_at_head(head_page_id, "Rohan Dvivedi", &lpltd,  pam_p, pmm_p);
	push_at_head(head_page_id, "Rupa Dvivedi", &lpltd,  pam_p, pmm_p);
	push_at_head(head_page_id, "Milan Dvivedi", &lpltd,  pam_p, pmm_p);

	print_all_forward(head_page_id, &lpltd, pam_p, NULL);
	print_all_reverse(head_page_id, &lpltd, pam_p, NULL);

	push_at_tail(head_page_id, "Rohan Dvivedi", &lpltd,  pam_p, pmm_p);
	push_at_tail(head_page_id, "Rupa Dvivedi", &lpltd,  pam_p, pmm_p);
	push_at_tail(head_page_id, "Milan Dvivedi", &lpltd,  pam_p, pmm_p);

	print_all_forward(head_page_id, &lpltd, pam_p, NULL);
	print_all_reverse(head_page_id, &lpltd, pam_p, NULL);

	push_at_tail(head_page_id, "My Dear Jijaji", &lpltd,  pam_p, pmm_p);
	push_at_tail(head_page_id, "Devashree joshi", &lpltd,  pam_p, pmm_p);
	push_at_tail(head_page_id, "Devashree Dvivedi", &lpltd,  pam_p, pmm_p);

	push_at_head(head_page_id, "Manan Joshi", &lpltd,  pam_p, pmm_p);
	push_at_head(head_page_id, "Vipulkumar Dvivedi", &lpltd,  pam_p, pmm_p);

	print_all_forward(head_page_id, &lpltd, pam_p, NULL);

	pop_from_head(head_page_id, 2, &lpltd, pam_p, pmm_p);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	// test below case with index values = 3 or 7
	update_from_head(head_page_id, 3, "Rohan Devashree", &lpltd, pam_p, pmm_p);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	// test with next merges properly
	print_all_forward(head_page_id, &lpltd, pam_p, pmm_p);
	//print_all_reverse(head_page_id, &lpltd, pam_p, pmm_p);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	pop_from_tail(head_page_id, 2, &lpltd, pam_p, pmm_p);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	int total_updates = reindex_all(head_page_id, 3, "X", &lpltd, pam_p, pmm_p);
	//int total_updates = reindex_all_reverse(head_page_id, 3, "X", &lpltd, pam_p, pmm_p);
	printf("total_updates = %d\n\n", total_updates);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	//int total_updates = reindex_all(head_page_id, 3, "X", &lpltd, pam_p, pmm_p);
	total_updates = reindex_all_reverse(head_page_id, 1, "Y", &lpltd, pam_p, pmm_p);
	printf("total_updates = %d\n\n", total_updates);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	total_updates = reindex_all_reverse(head_page_id, 10, NULL, &lpltd, pam_p, pmm_p);
	printf("total_updates = %d\n\n", total_updates);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	pop_from_tail(head_page_id, 20, &lpltd, pam_p, pmm_p);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	/* CLEANUP */

	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

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