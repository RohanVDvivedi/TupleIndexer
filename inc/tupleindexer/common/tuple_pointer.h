#ifndef TUPLE_POINTER_H
#define TUPLE_POINTER_H

#include<tupleindexer/common/page_access_specification.h>

typedef struct tuple_pointer tuple_pointer;
struct tuple_pointer
{
	uint64_t page_id;
	uint32_t tuple_index;
};

int is_tuple_pointer_NULL(const void* tptr_tpl, const page_access_specs* pas_p);

tuple_pointer get_tuple_pointer(const void* tptr_tpl, const page_access_specs* pas_p);

void set_tuple_pointer(void* tptr_tpl, tuple_pointer tptr, const page_access_specs* pas_p);

#endif