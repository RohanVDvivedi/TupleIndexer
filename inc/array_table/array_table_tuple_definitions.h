#ifndef PAGE_TABLE_TUPLE_DEFINITIONS_H
#define PAGE_TABLE_TUPLE_DEFINITIONS_H

#include<page_table_tuple_definitions_public.h>

// if return == 1, after the call to this function, result will be set to entries_per_page ^ exp
// on an overflow return == 0
int get_power_of_entries_per_page_using_page_table_tuple_definitions(const page_table_tuple_defs* pttd_p, uint64_t exp, uint64_t* result);

#endif