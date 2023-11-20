#include<page_table_page_header.h>

// number of bytes to store level of the page
// 2 is more than enough for curent computer systems, and allows us to store large amount of data
// values in range 1 to 4 both inclusive
#define BYTES_FOR_PAGE_LEVEL 2

uint32_t get_offset_to_end_of_page_table_page_header(const page_table_tuple_defs* pttd_p)
{
	return get_offset_to_end_of_common_page_header(&(pttd_p->pas)) + BYTES_FOR_PAGE_LEVEL + pttd_p->pas.page_id_width;
}

uint32_t get_level_of_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	return get_page_table_page_header(ppage, pttd_p).level;
}

uint64_t get_first_bucket_id_of_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	return get_page_table_page_header(ppage, pttd_p).first_bucket_id;
}

int is_page_table_leaf_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	return get_page_table_page_header(ppage, pttd_p).level == 0;
}

page_table_page_header get_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);

void serialize_page_table_page_header(void* hdr_serial, const page_table_page_header* bptiph_p, const page_table_tuple_defs* pttd_p);

void set_page_table_page_header(persistent_page* ppage, const page_table_page_header* bptiph_p, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);