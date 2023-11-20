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

	// this is the additional page header space left out by the library for your use
	// any page that will be used by the library for the bplus_tree will have page_header of size system_header_size of this plus the ones additionally required by the specific page type
	// this many number of bytes will be left in the preface of the page_header and will be left untouched
	// this part of the header can be used for storing :
	// * pageLSN (latest log_sequence_number that modified the page) for idempotency of physiological logs
	// * checksum (lets say crc32 of the whole page) for integrity
	// * transaction_id (to lock the whole page for writing - hypothetical, because you may just have latches managed by dam)
	uint32_t system_header_size;
};

// if this function returns 0, that implies that the pas's passed params are invalid
int is_valid_page_access_specs(const page_access_specs* pas_p);

void print_page_access_specs(const page_access_specs* pas_p);

#endif