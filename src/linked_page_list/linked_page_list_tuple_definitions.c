#include<tupleindexer/linked_page_list/linked_page_list_tuple_definitions.h>

#include<tupleindexer/linked_page_list/linked_page_list_page_header.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<stdlib.h>

int init_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p, const page_access_specs* pas_p, const tuple_def* record_def)
{
	// zero initialize lpltd_p
	(*lpltd_p) = (linked_page_list_tuple_defs){};

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs fo the bpttd
	lpltd_p->pas_p = pas_p;

	// this can only be done after setting the pas_p attribute of lpltd
	// there must be room for atleast some bytes after the page_table_page_header
	if(!can_page_header_fit_on_persistent_page(sizeof_LINKED_PAGE_LIST_PAGE_HEADER(lpltd_p), lpltd_p->pas_p->page_size))
		return 0;

	// check if the record_def's record's min_size fits on half of the page
	uint32_t space_allotted_for_records = get_space_to_be_allotted_to_all_tuples_on_persistent_page(sizeof_LINKED_PAGE_LIST_PAGE_HEADER(lpltd_p), pas_p->page_size, &(record_def->size_def));
	uint32_t space_additional_for_record = get_additional_space_overhead_per_tuple_on_persistent_page(pas_p->page_size, &(record_def->size_def));
	if((space_allotted_for_records / 2) < get_minimum_tuple_size(record_def) + space_additional_for_record)
		return 0;

	// calculate maximum record size that can can fit on this linked_page_list
	lpltd_p->max_record_size = (space_allotted_for_records / 2) - space_additional_for_record;
	if(is_fixed_sized_tuple_def(record_def))
		lpltd_p->max_record_size = record_def->size_def.size;

	// initialize record_def from the record_def provided
	lpltd_p->record_def = record_def;

	return 1;
}

int check_if_record_can_be_inserted_for_linked_page_list_tuple_definitions(const linked_page_list_tuple_defs* lpltd_p, const void* record_tuple)
{
	// if the record tuple is NULL, it is always insertable in linked_page_list
	if(record_tuple == NULL)
		return 1;

	uint32_t record_tuple_size = get_tuple_size(lpltd_p->record_def, record_tuple);

	// if the size of the record tuple is greater than the max_record_size of the lpltd, then it can not be inserted into the linked_page_list with the given lpltd
	if(record_tuple_size > lpltd_p->max_record_size)
		return 0;

	return 1;
}

void deinit_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p)
{
	// reset all attributes to NULL or 0
	lpltd_p->pas_p = NULL;
	lpltd_p->record_def = NULL;
	lpltd_p->max_record_size = 0;
}

void print_linked_page_list_tuple_definitions(const linked_page_list_tuple_defs* lpltd_p)
{
	printf("Linked_page_list tuple defs:\n");

	if(lpltd_p->pas_p)
		print_page_access_specs(lpltd_p->pas_p);

	printf("record_def = ");
	if(lpltd_p->record_def)
		print_tuple_def(lpltd_p->record_def);
	else
		printf("NULL\n");
}