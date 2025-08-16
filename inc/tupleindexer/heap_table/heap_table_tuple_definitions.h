#ifndef HEAP_TABLE_TUPLE_DEFINITIONS_H
#define HEAP_TABLE_TUPLE_DEFINITIONS_H

#include<tupleindexer/heap_table/heap_table_tuple_definitions_public.h>

#define HEAP_TABLE_ENTRY_TUPLE_MAX_SIZE (UINT32_C(4) + UINT32_C(8))
// heap_table entry for its bplus_tree is a fixed sized tuple where there can be 4 bytes of unused_space and 8 bytes of page_id

void build_heap_table_entry_tuple(const heap_table_tuple_defs* httd_p, void* entry_tuple, uint32_t unused_space, uint64_t page_id);

#endif