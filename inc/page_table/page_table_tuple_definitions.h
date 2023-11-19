#ifndef PAGE_TABLE_TUPLE_DEFINITIONS_H
#define PAGE_TABLE_TUPLE_DEFINITIONS_H

typedef struct page_table_tuple_defs page_table_tuple_defs;
struct page_table_tuple_defs
{
	// page_id_witdh is bytes required for storing page_id, it can be in range [1, 8] both inclusive
	uint8_t page_id_width;

	// size of each page inside this bplus tree
	uint32_t page_size;

	// this is what is considered as a NULL pointer in the bplus_tree
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

	// tuple definition of all the pages in the page_table
	tuple_def* entry_def;
	// additionally each entry is just a UINT value of page_id_width in the entry_def

	// this decides the number of entries that can fit on any of the page_table page
	// this is fixed since the entry_def is fixed sized
	uint64_t entries_per_page;
};

// initializes the attributes in page_table_tuple_defs struct as per the provided parameters
// it allocates memory only for entry_def
// returns 1 for success, it fails with 0
// it may also fail if the system_header size makes it impossible to store any entries on the page
// page_id_width is bytes required for storing page_id, it can be anything from 1 to 8 both inclusive
int init_bplus_tree_tuple_definitions(page_table_tuple_defs* pttd_p, uint32_t system_header_size, uint32_t page_size, uint8_t page_id_width, uint64_t NULL_PAGE_ID);

// it deallocates the entry_def and
// then resets all the page_table_tuple_defs struct attributes to NULL or 0
void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p);

// print page_table_tree_tuple_defs
void print_page_table_tuple_definitions(page_table_tuple_defs* pttd_p);

#endif