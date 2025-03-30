#include<tupleindexer/page_table/page_table_tuple_definitions.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<cutlery/cutlery_math.h>

#include<stdlib.h>

int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p)
{
	pttd_p->pas_p = pas_p;

	return init_array_table_tuple_definitions(&(pttd_p->attd), pas_p, &(pas_p->page_id_tuple_def));
}

void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{
	deinit_array_table_tuple_definitions(&(pttd_p->attd));
}

void print_page_table_tuple_definitions(const page_table_tuple_defs* pttd_p)
{
	print_array_table_tuple_definitions(&(pttd_p->attd));
}