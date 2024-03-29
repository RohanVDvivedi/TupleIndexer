#ifndef PAGE_TABLE_PAGE_HEADER_H
#define PAGE_TABLE_PAGE_HEADER_H

#include<common_page_header.h>
#include<page_table_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page.h>

typedef struct page_table_page_header page_table_page_header;
struct page_table_page_header
{
	common_page_header parent;

	// level of the page in page_table,
	// level == 0 -> leaf page
	// level  > 0 -> interior page
	uint32_t level;

	// first bucket that gets pointed to by this page's subtree
	uint64_t first_bucket_id;
};

#define sizeof_PAGE_TABLE_PAGE_HEADER get_offset_to_end_of_page_table_page_header

uint32_t get_offset_to_end_of_page_table_page_header(const page_table_tuple_defs* pttd_p);

uint32_t get_level_of_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

uint64_t get_first_bucket_id_of_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

int is_page_table_leaf_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

page_table_page_header get_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

void serialize_page_table_page_header(void* hdr_serial, const page_table_page_header* ptph_p, const page_table_tuple_defs* pttd_p);

void set_page_table_page_header(persistent_page* ppage, const page_table_page_header* ptph_p, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

#endif