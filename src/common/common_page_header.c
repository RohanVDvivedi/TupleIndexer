#include<common_page_header.h>

#include<persistent_page_functions.h>

#include<serial_int.h>

#include<cutlery_stds.h>

#include<stdlib.h>

const char* page_type_string[] = {
	"BPLUS_TREE_LEAF_PAGE",
	"BPLUS_TREE_INTERIOR_PAGE",
	"ARRAY_TABLE_PAGE",
	"LINKED_PAGE_LIST_PAGE",
};

// allowed values for size for storing page_type = 1 or 2
#define BYTES_FOR_PAGE_TYPE 2

uint32_t get_offset_to_end_of_common_page_header(const page_access_specs* pas_p)
{
	return pas_p->system_header_size + BYTES_FOR_PAGE_TYPE;
}

page_type get_type_of_page(const persistent_page* ppage, const page_access_specs* pas_p)
{
	return get_common_page_header(ppage, pas_p).type;
}

static inline uint32_t get_offset_to_common_page_header_locals(const page_access_specs* pas_p)
{
	return pas_p->system_header_size;
}

common_page_header get_common_page_header(const persistent_page* ppage, const page_access_specs* pas_p)
{
	const void* common_page_header_serial = get_page_header_ua_persistent_page(ppage, pas_p->page_size) + get_offset_to_common_page_header_locals(pas_p);
	return (common_page_header){
		.type = (page_type) deserialize_uint16(common_page_header_serial, BYTES_FOR_PAGE_TYPE),
	};
}

void serialize_common_page_header(void* hdr_serial, const common_page_header* cph_p, const page_access_specs* pas_p)
{
	void* common_page_header_serial = hdr_serial + get_offset_to_common_page_header_locals(pas_p);
	serialize_uint16(common_page_header_serial, BYTES_FOR_PAGE_TYPE, ((uint16_t)(cph_p->type)));
}

void set_common_page_header(persistent_page* ppage, const common_page_header* cph_p, const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, pas_p->page_size), page_header_size);

	// serialize cph_p on the hdr_serial
	serialize_common_page_header(hdr_serial, cph_p, pas_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

void print_common_page_header(const persistent_page* ppage, const page_access_specs* pas_p)
{
	printf("page_type : %s\n", page_type_string[get_type_of_page(ppage, pas_p)]);
}