#include<tupleindexer/bitmap_page/bitmap_page.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<stdlib.h>

tuple_def* get_tuple_definition_for_bitmap_page(const page_access_specs* pas_p, uint8_t bits_per_field, uint64_t* elements_per_page)
{
	if(bits_per_field == 0 || bits_per_field > 64)
		return NULL;

	data_type_info* array_of_bit_fields_type = malloc(sizeof(data_type_info));
	if(array_of_bit_fields_type == NULL)
		exit(-1);

	// calculate total number of bytes and bits that can fit on this page
	uint32_t bytes_count = get_maximum_tuple_size_accomodatable_on_persistent_page(uint32_t page_header_size, pas_p->page_size, const tuple_size_def* tpl_sz_d);
	uint64_t bits_count = (bytes_count * 8);

	(*elements_per_page) = bits_count / bits_per_field;

	(*array_of_bit_fields_type) = get_fixed_element_count_array_type("BIT_FIELD_ARRAY", (*elements_per_page), 0, 0, BIT_FIELD_NON_NULLABLE[bits_per_field]);

	tuple_def* bitmap_page_def = malloc(sizeof(tuple_def));
	if(bitmap_page_def == NULL)
		exit(-1);

	initialize_tuple_def(bitmap_page_def, array_of_bit_fields_type);

	return bitmap_page_def;
}