#ifndef PAGE_ACCESS_SPECIFICATION_H
#define PAGE_ACCESS_SPECIFICATION_H

#include<inttypes.h>

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
};

// if this function returns 0, that implies that the pas's passed params are invalid
int is_valid_page_access_specs(const page_access_specs* pas_p);

void print_page_access_specs(const page_access_specs* pas_p);

#endif