#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/linked_page_list/linked_page_list.h>

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

int insert_counter = 0;

int push_at_head(uint64_t head_page_id, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	char tuple[64] = {};
	initialize_tuple(lpltd_p->record_def, tuple, insert_counter++, name);
	int res = insert_at_linked_page_list_iterator(lpli_p, tuple, INSERT_BEFORE_LINKED_PAGE_LIST_ITERATOR, transaction_id, &abort_error);
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

	return res;
}

int push_at_tail(uint64_t head_page_id, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
	int res = insert_at_linked_page_list_iterator(lpli_p, tuple, INSERT_AFTER_LINKED_PAGE_LIST_ITERATOR, transaction_id, &abort_error);
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

	return res;
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

			updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, STATIC_POSITION(0), &((user_value){.int_value = insert_counter++}), transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(name != NULL)
			{
				updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, STATIC_POSITION(1), &((user_value){.string_value = name, .string_size = strlen(name)}), transaction_id, &abort_error);
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

			updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, STATIC_POSITION(0), &((user_value){.int_value = insert_counter++}), transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			if(name != NULL)
			{
				updated += update_element_in_place_at_linked_page_list_iterator(lpli_p, STATIC_POSITION(1), &((user_value){.string_value = name, .string_size = strlen(name)}), transaction_id, &abort_error);
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

int update_all_to_identical_from_head(uint64_t head_page_id, char* name, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
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
			printf("updated from : ");
			print_tuple(get_tuple_linked_page_list_iterator(lpli_p), lpltd_p->record_def);

			char tuple[64] = {};
			initialize_tuple(lpltd_p->record_def, tuple, insert_counter++, name);
			updated += update_at_linked_page_list_iterator(lpli_p, tuple, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}

			printf("updated to : ");
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

	return updated;
}

void multiple_active_iterators_to_print(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p)
{
	linked_page_list_iterator* lpli1_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, NULL, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(is_empty_linked_page_list(lpli1_p))
	{
		delete_linked_page_list_iterator(lpli1_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		return;
	}

	prev_linked_page_list_iterator(lpli1_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	linked_page_list_iterator* lpli2_p = clone_linked_page_list_iterator(lpli1_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	next_linked_page_list_iterator(lpli2_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	do
	{
		printf("I1 -> ");
		print_tuple(get_tuple_linked_page_list_iterator(lpli1_p), lpltd_p->record_def);

		printf("I2 -> ");
		print_tuple(get_tuple_linked_page_list_iterator(lpli2_p), lpltd_p->record_def);

		next_linked_page_list_iterator(lpli1_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		next_linked_page_list_iterator(lpli2_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}
	while(!is_at_head_tuple_linked_page_list_iterator(lpli2_p));

	delete_linked_page_list_iterator(lpli1_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_linked_page_list_iterator(lpli2_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

// this function will block you forever (just or test)
void test_concurrency(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	linked_page_list_iterator* lpli1_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	prev_linked_page_list_iterator(lpli1_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("lpli1 is at tail page = %d\n", is_at_tail_page_linked_page_list_iterator(lpli1_p));

	// this will block here forever, if pmm_p != NULL
	linked_page_list_iterator* lpli2_p = get_new_linked_page_list_iterator(head_page_id, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("lpli2 is at head page = %d\n", is_at_head_page_linked_page_list_iterator(lpli2_p));

	delete_linked_page_list_iterator(lpli1_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	delete_linked_page_list_iterator(lpli2_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	printf("\n\n");
}

void test_merge(const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	printf("------------------------------TEST MERGE-------------------------------\n");
	uint64_t head_page_id = get_new_linked_page_list(lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);

	{
		printf("both are empty\n");
		uint64_t head_page_id2 = get_new_linked_page_list(lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		merge_linked_page_lists(head_page_id, head_page_id2, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		print_linked_page_list(head_page_id, lpltd_p, pam_p, transaction_id, &abort_error);
		printf("\n\n");
	}

	{
		printf("original is empty, another one is not\n");
		uint64_t head_page_id2 = get_new_linked_page_list(lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		push_at_tail(head_page_id2, "1Rohan Dvivedi1", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "2Rupa Dvivedi1", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "3Milan Dvivedi1", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "4Devashree Dvivedi1", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "5Devashree Dvivedi1-------", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "6My dearest Manan Joshi1-------", lpltd_p,  pam_p, pmm_p);
		/*push_at_tail(head_page_id2, "7Rohan Dvivedi3", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "8Rupa Dvivedi3", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "9Milan Dvivedi3", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "10Devashree Dvivedi3", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "11Devashree Dvivedi3-------", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "12My dearest Manan Joshi3-------", lpltd_p,  pam_p, pmm_p);*/
		merge_linked_page_lists(head_page_id, head_page_id2, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		print_linked_page_list(head_page_id, lpltd_p, pam_p, transaction_id, &abort_error);
		printf("\n\n");
	}

	{
		printf("original is not empty, another is empty\n");
		uint64_t head_page_id2 = get_new_linked_page_list(lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		merge_linked_page_lists(head_page_id, head_page_id2, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		print_linked_page_list(head_page_id, lpltd_p, pam_p, transaction_id, &abort_error);
		printf("\n\n");
	}

	{
		printf("both are not empty\n");
		uint64_t head_page_id2 = get_new_linked_page_list(lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		push_at_tail(head_page_id2, "1Rohan Dvivedi2", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "2Rupa Dvivedi2", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "3Milan Dvivedi2", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "4Devashree Dvivedi2", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "5Devashree Dvivedi2-------", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "6My dearest Manan Joshi2-------", lpltd_p,  pam_p, pmm_p);
		/*push_at_tail(head_page_id2, "7Rohan Dvivedi4", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "8Rupa Dvivedi4", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "9Milan Dvivedi4", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "10Devashree Dvivedi4", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "11Devashree Dvivedi4-------", lpltd_p,  pam_p, pmm_p);
		push_at_tail(head_page_id2, "12My dearest Manan Joshi4-------", lpltd_p,  pam_p, pmm_p);*/
		merge_linked_page_lists(head_page_id, head_page_id2, lpltd_p, pam_p, pmm_p, transaction_id, &abort_error);
		print_linked_page_list(head_page_id, lpltd_p, pam_p, transaction_id, &abort_error);
		printf("\n\n");
	}

	// destroy original linked_page_list
	destroy_linked_page_list(head_page_id, lpltd_p, pam_p, transaction_id, &abort_error);

	printf("-------------------------------TEST MERGE-complete--------------------------------\n");
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

	// below lines blocks because of write locks, if pmm_p != NULL
	//print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);
	//test_concurrency(head_page_id, &lpltd, pam_p, NULL);

	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);
	multiple_active_iterators_to_print(head_page_id, &lpltd, pam_p);

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

	total_updates = update_all_to_identical_from_head(head_page_id, "Rohan Rupa Vipul Devashree Manan", &lpltd, pam_p, pmm_p);
	printf("total_updates = %d\n\n", total_updates);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	pop_from_head(head_page_id, 20, &lpltd, pam_p, pmm_p);
	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	push_at_tail(head_page_id, "My Dear, Manan Jijaji", &lpltd,  pam_p, pmm_p);

	// attempt to insert a NULL
	int r = 0;
	{
		linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(head_page_id, &lpltd, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}

		r = insert_at_linked_page_list_iterator(lpli_p, NULL, INSERT_BEFORE_LINKED_PAGE_LIST_ITERATOR, transaction_id, &abort_error);
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
	printf("result of inserting NULL tuple = %d\n", r);

	r = push_at_head(head_page_id, "qwertyuiopasdfghjklzxcvbnm/qwertyuiopasdfghjklzxcvbnm/", &lpltd,  pam_p, pmm_p);
	printf("result of inserting too long tuple tuple = %d\n", r);

	/* CLEANUP */

	print_linked_page_list(head_page_id, &lpltd, pam_p, transaction_id, &abort_error);

	test_merge(&lpltd, pam_p, pmm_p);

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

	return 0;
}