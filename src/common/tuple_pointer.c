#include<tupleindexer/common/tuple_pointer.h>

#include<tuplestore/tuple.h>

int is_tuple_pointer_NULL(const void* tptr_tpl, const page_access_specs* pas_p)
{
	datum uval;
	get_value_from_element_from_tuple(&uval, &(pas_p->tuple_pointer_tuple_def), STATIC_POSITION(0), tptr_tpl);

	return (uval.uint_value == pas_p->NULL_PAGE_ID);
}

tuple_pointer get_tuple_pointer(const void* tptr_tpl, const page_access_specs* pas_p)
{
	tuple_pointer tptr;

	datum uval;

	get_value_from_element_from_tuple(&uval, &(pas_p->tuple_pointer_tuple_def), STATIC_POSITION(0), tptr_tpl);
	tptr.page_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, &(pas_p->tuple_pointer_tuple_def), STATIC_POSITION(1), tptr_tpl);
	tptr.tuple_index = uval.uint_value;

	return tptr;
}

void set_tuple_pointer(void* tptr_tpl, tuple_pointer tptr, const page_access_specs* pas_p)
{
	set_element_in_tuple(&(pas_p->tuple_pointer_tuple_def), STATIC_POSITION(0), tptr_tpl, &((const datum){.uint_value = tptr.page_id}), 0);
	set_element_in_tuple(&(pas_p->tuple_pointer_tuple_def), STATIC_POSITION(1), tptr_tpl, &((const datum){.uint_value = tptr.tuple_index}), 0);
}