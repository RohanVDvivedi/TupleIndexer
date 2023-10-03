#include<common_page_header.h>

#include<page_layout_unaltered.h>
#include<int_accesses.h>

#include<cutlery_stds.h>

#include<stdlib.h>

const char* page_type_string[] = {
	"BPLUS_TREE_LEAF_PAGE",
	"BPLUS_TREE_INTERIOR_PAGE",
};

// allowed values for size for storing page_type = 1 or 2
#define BYTES_FOR_PAGE_TYPE 2

uint32_t get_offset_to_end_of_common_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return bpttd_p->system_header_size + BYTES_FOR_PAGE_TYPE;
}

page_type get_type_of_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_common_page_header(page, bpttd_p).type;
}

static inline uint32_t get_offset_to_common_page_header_locals(const bplus_tree_tuple_defs* bpttd_p)
{
	return bpttd_p->system_header_size;
}

common_page_header get_common_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* common_page_header_serial = get_page_header_ua((void*)page, bpttd_p->page_size) + get_offset_to_common_page_header_locals(bpttd_p);
	return (common_page_header){
		.type = (page_type) read_uint16(common_page_header_serial, BYTES_FOR_PAGE_TYPE),
	};
}

void serialize_common_page_header(void* hdr_serial, const common_page_header* cph_p, const bplus_tree_tuple_defs* bpttd_p)
{
	void* common_page_header_serial = hdr_serial + get_offset_to_common_page_header_locals(bpttd_p);
	write_uint16(common_page_header_serial, BYTES_FOR_PAGE_TYPE, ((uint16_t)(cph_p->type)));
}

void set_common_page_header(persistent_page ppage, const common_page_header* cph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p)
{
	uint32_t page_header_size = get_page_header_size(ppage.page, bpttd_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua(ppage.page, bpttd_p->page_size), page_header_size);

	// serialize cph_p on the hdr_serial
	serialize_common_page_header(hdr_serial, cph_p, bpttd_p);

	// write hdr_serial to the new header position
	pmm_p->set_page_header(pmm_p->context, ppage, bpttd_p->page_size, hdr_serial);

	free(hdr_serial);
}

void print_common_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	printf("page_type : %s\n", page_type_string[get_type_of_page(page, bpttd_p)]);
}