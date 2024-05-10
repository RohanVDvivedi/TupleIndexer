#include<linked_page_list_tuple_definitions.h>

#include<linked_page_list_page_header.h>

#include<persistent_page_functions.h>

int init_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p, const page_access_specs* pas_p, const tuple_def* record_def)
{
	// basic parameter check
	if(get_element_def_count_tuple_def(record_def) == 0)
		return 0;

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs fo the bpttd
	lpltd_p->pas_p = pas_p;

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
	lpltd_p->record_def = clone_tuple_def(record_def);
	if(bpttd_p->record_def == NULL) // memory allocation failed
		exit(-1);
	finalize_tuple_def(lpltd_p->record_def);

	return 1;
}

void deinit_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p)
{
	// delete tuple_def if it exists
	if(lpltd_p->record_def)
		delete_tuple_def(lpltd_p->record_def);

	// reset all attributes to NULL or 0
	lpltd_p->pas_p = NULL;
	lpltd_p->record_def = NULL;
	lpltd_p->max_record_size = 0;
}

void print_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p)
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