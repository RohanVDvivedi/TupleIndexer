#include<page_table_page_header.h>

#include<persistent_page_functions.h>

#include<serial_int.h>

#include<stdlib.h>

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

static inline uint32_t get_offset_to_page_table_page_header_locals(const page_table_tuple_defs* pttd_p)
{
	return get_offset_to_end_of_common_page_header(&(pttd_p->pas));
}

page_table_page_header get_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	const void* page_table_page_header_serial = get_page_header_ua_persistent_page(ppage, pttd_p->pas.page_size) + get_offset_to_page_table_page_header_locals(pttd_p);
	return (page_table_page_header){
		.parent = get_common_page_header(ppage, &(pttd_p->pas)),
		.level = deserialize_uint32(page_table_page_header_serial, BYTES_FOR_PAGE_LEVEL),
		.first_bucket_id = deserialize_uint64(page_table_page_header_serial + BYTES_FOR_PAGE_LEVEL, pttd_p->pas.page_id_width),
	};
}

void serialize_page_table_page_header(void* hdr_serial, const page_table_page_header* ptph_p, const page_table_tuple_defs* pttd_p);

void set_page_table_page_header(persistent_page* ppage, const page_table_page_header* ptph_p, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p);