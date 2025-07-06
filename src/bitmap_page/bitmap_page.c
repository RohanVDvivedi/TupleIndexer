#include<tupleindexer/bitmap_page/bitmap_page.h>

#include<tupleindexer/utils/persistent_page_functions.h>
#include<tupleindexer/bitmap_page/bitmap_page_header.h>

#include<stdlib.h>

tuple_def* get_tuple_definition_for_bitmap_page(const page_access_specs* pas_p, uint8_t bits_per_field, uint64_t* elements_per_page)
{
	if(bits_per_field == 0 || bits_per_field > 64)
		return NULL;

	// calculate total number of bytes and bits that can fit on this page
	uint32_t bytes_count = get_maximum_tuple_size_accomodatable_on_persistent_page(sizeof_BITMAP_PAGE_HEADER(pas_p), pas_p->page_size, &((tuple_size_def){.is_variable_sized = 0}));
	uint64_t bits_count = (bytes_count * 8);

	(*elements_per_page) = bits_count / bits_per_field;

	// make sure that atleast 1 element can fit on the page, else fail this call
	if((*elements_per_page) == 0)
		return NULL;

	data_type_info* array_of_bit_fields_type = malloc(sizeof(data_type_info));
	if(array_of_bit_fields_type == NULL)
		exit(-1);

	(*array_of_bit_fields_type) = get_fixed_element_count_array_type("BIT_FIELD_ARRAY", (*elements_per_page), 0, 0, BIT_FIELD_NON_NULLABLE[bits_per_field]);

	tuple_def* bitmap_page_def = malloc(sizeof(tuple_def));
	if(bitmap_page_def == NULL)
		exit(-1);

	initialize_tuple_def(bitmap_page_def, array_of_bit_fields_type);

	return bitmap_page_def;
}

persistent_page get_new_bitmap_page_with_write_lock(const page_access_specs* pas_p, const tuple_def* tpl_d, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	persistent_page bitmap_page = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);
	if(*abort_error)
		return bitmap_page;

	int inited = init_persistent_page(pmm_p, transaction_id, &bitmap_page, pas_p->page_size, sizeof_BITMAP_PAGE_HEADER(pas_p), &(tpl_d->size_def), abort_error);
	if((*abort_error) || !inited)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &bitmap_page, NONE_OPTION, abort_error);
		return bitmap_page;
	}

	// get the header, initialize it and set it back on to the page
	bitmap_page_header hdr = get_bitmap_page_header(&bitmap_page, pas_p);
	hdr.parent.type = BITMAP_PAGE; // always a heap page
	set_bitmap_page_header(&bitmap_page, &hdr, pas_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &bitmap_page, NONE_OPTION, abort_error);
		return bitmap_page;
	}

	// insert a tuple on the page consisting of all 0 bit fields
	{
		char* zero_bits_tuple = malloc(tpl_d->size_def.size);
		init_tuple(tpl_d, zero_bits_tuple);

		uint32_t element_count = get_element_count_for_element_from_tuple(tpl_d, SELF, zero_bits_tuple);

		for(uint32_t i = 0; i < element_count; i++)
			set_element_in_tuple(tpl_d, STATIC_POSITION(i), zero_bits_tuple, ZERO_USER_VALUE, 0);

		append_tuple_on_persistent_page(pmm_p, transaction_id, &bitmap_page, pas_p->page_size, &(tpl_d->size_def), zero_bits_tuple, abort_error);
		free(zero_bits_tuple);
		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &bitmap_page, NONE_OPTION, abort_error);
			return bitmap_page;
		}
	}

	return bitmap_page;
}

int set_bit_field_on_bitmap_page(persistent_page* ppage, uint32_t index, uint64_t value, const page_access_specs* pas_p, const tuple_def* tpl_d, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int res = set_element_in_tuple_in_place_on_persistent_page(pmm_p, transaction_id, ppage, pas_p->page_size, tpl_d, 0, STATIC_POSITION(index), &((user_value){.bit_field_value = value}), abort_error);
	if(*abort_error)
		return 0;
	return res;
}

uint64_t get_bit_field_on_bitmap_page(const persistent_page* ppage, uint32_t index, const page_access_specs* pas_p, const tuple_def* tpl_d, const void* transaction_id, int* abort_error)
{
	// get the one and only tuple on the page
	const void* bitmap_page_only_tuple = get_nth_tuple_on_persistent_page(ppage, pas_p->page_size, &(tpl_d->size_def), 0);
	if(bitmap_page_only_tuple == NULL)
		return 0;

	user_value uval;
	if(!get_value_from_element_from_tuple(&uval, tpl_d, STATIC_POSITION(index), bitmap_page_only_tuple) == 0)
		return 0;

	// his shall never happen
	if(is_user_value_NULL(&uval))
		return 0;

	return uval.bit_field_value;
}

void print_bitmap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d)
{
	print_bitmap_page_header(ppage, pas_p);
	print_persistent_page(ppage, pas_p->page_size, tpl_d);
}