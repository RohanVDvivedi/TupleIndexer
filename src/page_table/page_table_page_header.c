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
	return get_offset_to_end_of_common_page_header(pttd_p->pas_p) + BYTES_FOR_PAGE_LEVEL + pttd_p->pas_p->page_id_width;
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
	return get_offset_to_end_of_common_page_header(pttd_p->pas_p);
}

page_table_page_header get_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	const void* page_table_page_header_serial = get_page_header_ua_persistent_page(ppage, pttd_p->pas_p->page_size) + get_offset_to_page_table_page_header_locals(pttd_p);
	return (page_table_page_header){
		.parent = get_common_page_header(ppage, pttd_p->pas_p),
		.level = deserialize_uint32(page_table_page_header_serial, BYTES_FOR_PAGE_LEVEL),
		.first_bucket_id = deserialize_uint64(page_table_page_header_serial + BYTES_FOR_PAGE_LEVEL, pttd_p->pas_p->page_id_width),
	};
}

void serialize_page_table_page_header(void* hdr_serial, const page_table_page_header* ptph_p, const page_table_tuple_defs* pttd_p)
{
	serialize_common_page_header(hdr_serial, &(ptph_p->parent), pttd_p->pas_p);

	void* page_table_page_header_serial = hdr_serial + get_offset_to_page_table_page_header_locals(pttd_p);
	serialize_uint32(page_table_page_header_serial, BYTES_FOR_PAGE_LEVEL, ptph_p->level);
	serialize_uint64(page_table_page_header_serial + BYTES_FOR_PAGE_LEVEL, pttd_p->pas_p->page_id_width, ptph_p->first_bucket_id);
}

void set_page_table_page_header(persistent_page* ppage, const page_table_page_header* ptph_p, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, pttd_p->pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, pttd_p->pas_p->page_size), page_header_size);

	// serialize ptph_p on the hdr_serial
	serialize_page_table_page_header(hdr_serial, ptph_p, pttd_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, pttd_p->pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

void print_page_table_page_header(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	print_common_page_header(ppage, pttd_p->pas_p);
	printf("level : %"PRIu32"\n", get_level_of_page_table_page(ppage, pttd_p));
	printf("first_bucket_id : %"PRIu64"\n", get_first_bucket_id_of_page_table_page(ppage, pttd_p));
}