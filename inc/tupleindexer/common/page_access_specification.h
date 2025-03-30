#ifndef PAGE_ACCESS_SPECIFICATION_H
#define PAGE_ACCESS_SPECIFICATION_H

#include<inttypes.h>
#include<tuple_def.h>

typedef struct page_access_specs page_access_specs;
struct page_access_specs
{
	// page_id_width is bytes required for storing page_id, it can be in range [1, 8] both inclusive
	uint8_t page_id_width;

	// size of each page that can be accessed
	uint32_t page_size;

	// this is what is considered as a NULL pointer for TupleStore
	// NULL_PAGE_ID < (1 << (page_id_width * 8))
	uint64_t NULL_PAGE_ID;

	// every page access spec defines the page_id type is it a NON NULLABLE unsigned integral type as wide as page_id_width

	// defines a non-nullable unsigned integral type of page_id_width bytes to store page_id-s
	data_type_info page_id_type_info;

	// helps in working with pages composed on only page_ids
	// the type_info of this tuple_Def must be the attribute above
	tuple_def page_id_tuple_def;
};

// initialize all attributed of page_access_specs
int initialize_page_access_specs(page_access_specs* pas_p, uint8_t page_id_width, uint32_t page_size, uint64_t NULL_PAGE_ID);

// if this function returns 0, that implies that the pas's passed params are invalid
int is_valid_page_access_specs(const page_access_specs* pas_p);

void print_page_access_specs(const page_access_specs* pas_p);

#endif