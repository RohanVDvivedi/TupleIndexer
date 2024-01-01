#include<linked_page_list_tuple_definitions.h>

int init_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p, const page_access_specs* pas_p, const tuple_def* record_def);

void deinit_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p);

void print_linked_page_list_tuple_definitions(linked_page_list_tuple_defs* lpltd_p);