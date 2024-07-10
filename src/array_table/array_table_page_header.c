#include<array_table_page_header.h>

#include<persistent_page_functions.h>

#include<serial_int.h>

#include<stdlib.h>

// number of bytes to store level of the page
// 2 is more than enough for curent computer systems, and allows us to store large amount of data
// values in range 1 to 4 both inclusive
#define BYTES_FOR_PAGE_LEVEL 2

uint32_t get_offset_to_end_of_array_table_page_header(const array_table_tuple_defs* attd_p)
{
	return get_offset_to_end_of_common_page_header(attd_p->pas_p) + BYTES_FOR_PAGE_LEVEL + attd_p->pas_p->page_id_width;
}

uint32_t get_level_of_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	return get_array_table_page_header(ppage, attd_p).level;
}

uint64_t get_first_bucket_id_of_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	return get_array_table_page_header(ppage, attd_p).first_bucket_id;
}

int is_array_table_leaf_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	return get_array_table_page_header(ppage, attd_p).level == 0;
}

static inline uint32_t get_offset_to_array_table_page_header_locals(const array_table_tuple_defs* attd_p)
{
	return get_offset_to_end_of_common_page_header(attd_p->pas_p);
}

array_table_page_header get_array_table_page_header(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	const void* array_table_page_header_serial = get_page_header_ua_persistent_page(ppage, attd_p->pas_p->page_size) + get_offset_to_array_table_page_header_locals(attd_p);
	return (array_table_page_header){
		.parent = get_common_page_header(ppage, attd_p->pas_p),
		.level = deserialize_uint32(array_table_page_header_serial, BYTES_FOR_PAGE_LEVEL),
		.first_bucket_id = deserialize_uint64(array_table_page_header_serial + BYTES_FOR_PAGE_LEVEL, attd_p->pas_p->page_id_width),
	};
}

void serialize_array_table_page_header(void* hdr_serial, const array_table_page_header* atph_p, const array_table_tuple_defs* attd_p)
{
	serialize_common_page_header(hdr_serial, &(atph_p->parent), attd_p->pas_p);

	void* array_table_page_header_serial = hdr_serial + get_offset_to_array_table_page_header_locals(attd_p);
	serialize_uint32(array_table_page_header_serial, BYTES_FOR_PAGE_LEVEL, atph_p->level);
	serialize_uint64(array_table_page_header_serial + BYTES_FOR_PAGE_LEVEL, attd_p->pas_p->page_id_width, atph_p->first_bucket_id);
}

void set_array_table_page_header(persistent_page* ppage, const array_table_page_header* atph_p, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, attd_p->pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, attd_p->pas_p->page_size), page_header_size);

	// serialize atph_p on the hdr_serial
	serialize_array_table_page_header(hdr_serial, atph_p, attd_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

void print_array_table_page_header(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	print_common_page_header(ppage, attd_p->pas_p);
	printf("level : %"PRIu32"\n", get_level_of_array_table_page(ppage, attd_p));
	printf("first_bucket_id : %"PRIu64"\n", get_first_bucket_id_of_array_table_page(ppage, attd_p));
}