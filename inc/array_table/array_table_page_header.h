#ifndef ARRAY_TABLE_PAGE_HEADER_H
#define ARRAY_TABLE_PAGE_HEADER_H

#include<common_page_header.h>
#include<array_table_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page.h>

typedef struct array_table_page_header array_table_page_header;
struct array_table_page_header
{
	common_page_header parent;

	// level of the page in array_table,
	// level == 0 -> leaf page
	// level  > 0 -> interior page
	uint32_t level;

	// first bucket that gets pointed to by this page's subtree
	uint64_t first_bucket_id;
};

#define sizeof_ARRAY_TABLE_PAGE_HEADER get_offset_to_end_of_array_table_page_header

uint32_t get_offset_to_end_of_array_table_page_header(const array_table_tuple_defs* attd_p);

uint32_t get_level_of_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

uint64_t get_first_bucket_id_of_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

int is_array_table_leaf_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

array_table_page_header get_array_table_page_header(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

void serialize_array_table_page_header(void* hdr_serial, const array_table_page_header* atph_p, const array_table_tuple_defs* attd_p);

void set_array_table_page_header(persistent_page* ppage, const array_table_page_header* atph_p, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_array_table_page_header(const persistent_page* ppage, const array_table_tuple_defs* attd_p);

#endif