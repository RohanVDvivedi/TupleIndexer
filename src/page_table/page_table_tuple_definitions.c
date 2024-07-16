#include<page_table_tuple_definitions.h>

#include<persistent_page_functions.h>

#include<cutlery_math.h>

#include<stdlib.h>

int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p)
{
	pttd_p->pas_p = pas_p;

	tuple_def* record_def = get_new_tuple_def("temp_page_table_record_def", 1, pttd_p->pas_p->page_size);
	if(record_def == NULL) // memory allocation failed
		exit(-1);
	insert_element_def(record_def, "page_id", UINT, pttd_p->pas_p->page_id_width, 1, &((user_value){.uint_value = pttd_p->pas_p->NULL_PAGE_ID}));

	int res = init_array_table_tuple_definitions(&(pttd_p->attd), pas_p, record_def);

	delete_tuple_def(record_def);

	return res;
}

void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{
	deinit_array_table_tuple_definitions(&(pttd_p->attd));
}

void print_page_table_tuple_definitions(const page_table_tuple_defs* pttd_p)
{
	print_array_table_tuple_definitions(&(pttd_p->attd));
}