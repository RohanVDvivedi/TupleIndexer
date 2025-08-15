#include<tupleindexer/heap_table/heap_table_tuple_definitions.h>

int init_hash_table_tuple_definitions(heap_table_tuple_defs* httd_p, const page_access_specs* pas_p, const tuple_def* record_def);

void deinit_heap_table_tuple_definitions(heap_table_tuple_defs* httd_p);

void print_heap_table_tuple_definitions(heap_table_tuple_defs* httd_p)
{
	printf("Heap_table tuple defs:\n");

	if(httd_p->pas_p)
		print_page_access_specs(httd_p->pas_p);

	printf("record_def = ");
	if(httd_p->record_def)
		print_tuple_def(httd_p->record_def);
	else
		printf("NULL\n");

	print_bplus_tree_tuple_definitions(&(httd_p->bpttd));
}