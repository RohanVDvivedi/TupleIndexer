#ifndef BITMAP_PAGE_H
#define BITMAP_PAGE_H

#include<tupleindexer/common/page_access_specification.h>

#include<tuplestore/tuple_def.h>

// returns the tuple_def for accessing a bitmap page, that is a fixed array page, consisting of only 1 tuple
// this tuple has a tuple_def having a fixed length array of non-nullable bit-fields that has bits_per_field in range [1, 64], number of bits
// the number of elements on the page depends on the size of the page and is also returned
tuple_def* get_tuple_definition_for_bitmap_page(const page_access_specs* pas_p, uint8_t bits_per_field, uint64_t* elements_per_page);

#endif