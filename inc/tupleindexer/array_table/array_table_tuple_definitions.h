#ifndef ARRAY_TABLE_TUPLE_DEFINITIONS_H
#define ARRAY_TABLE_TUPLE_DEFINITIONS_H

#include<tupleindexer/array_table/array_table_tuple_definitions_public.h>

// if return == 1, after the call to this function, result will be set to (leaf_entries_per_page) * (index_entries_per_page ^ (level - 1)) OR 1 if level == 0
// on an overflow return == 0
int get_leaf_entries_refrenceable_by_entry_at_given_level_using_array_table_tuple_definitions(const array_table_tuple_defs* attd_p, uint64_t level, uint64_t* result);

#endif