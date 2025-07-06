#ifndef BITMAP_PAGE_H
#define BITMAP_PAGE_H

#include<tupleindexer/common/page_access_specification.h>
#include<tupleindexer/utils/persistent_page_functions.h>

#include<tuplestore/tuple_def.h>

// returns the tuple_def for accessing a bitmap page, that is a fixed array page, consisting of only 1 tuple
// this tuple has a tuple_def having a fixed length array of non-nullable bit-fields that has bits_per_field in range [1, 64], number of bits
// the number of elements on the page depends on the size of the page and is also returned
// any access to bit_fields (using the get and set functions below) outside of (*elements_per_page) is an an out-of-bounds access
tuple_def* get_tuple_definition_for_bitmap_page(const page_access_specs* pas_p, uint8_t bits_per_field, uint64_t* elements_per_page);

persistent_page get_new_bitmap_page_with_write_lock(const page_access_specs* pas_p, const tuple_def* tpl_d, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// on the bit_field page sets value at the given index
// returns 0 on out of bounds access OR on an abort error
int set_bit_field_on_bitmap_page(persistent_page* ppage, uint32_t index, uint64_t value, const page_access_specs* pas_p, const tuple_def* tpl_d, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// on the bit_field page gets value at the given index
// returns 0 on out of bounds access OR on an abort error
uint64_t get_bit_field_on_bitmap_page(const persistent_page* ppage, uint32_t index, const page_access_specs* pas_p, const tuple_def* tpl_d, const void* transaction_id, int* abort_error);

void print_bitmap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d);

#endif