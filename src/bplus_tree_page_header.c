#include<bplus_tree_page_header.h>

#include<page_layout_unaltered.h>
#include<int_accesses.h>

#include<stdlib.h>

// number of bytes to store level of the page
// 2 is more than enough for curent computer systems, and allows us to store large amount of data
// values in range 1 to 4 both inclusive
#define BYTES_FOR_PAGE_LEVEL 2

uint32_t get_offset_of_bplus_tree_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_of_common_page_header(bpttd_p) + get_size_of_common_page_header();
}

uint32_t get_size_of_bplus_tree_page_header()
{
	return BYTES_FOR_PAGE_LEVEL;
}

/*
uint32_t get_level_of_bplus_tree_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* page_level_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_page_level_header(bpttd_p);
	return read_uint32(page_level_header, BYTES_FOR_PAGE_LEVEL);
}

void set_level_of_bplus_tree_page(void* page, uint32_t level, const bplus_tree_tuple_defs* bpttd_p)
{
	void* page_level_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_page_level_header(bpttd_p);
	write_uint32(page_level_header, BYTES_FOR_PAGE_LEVEL, level);
}
*/

uint32_t get_level_of_bplus_tree_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_page_header(page, bpttd_p).level;
}

int is_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_level_of_bplus_tree_page(page, bpttd_p) == 0;
}

bplus_tree_page_header get_bplus_tree_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* bplus_tree_page_header_serial = get_page_header_ua((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_page_header(bpttd_p);
	return (bplus_tree_page_header){
		.parent = get_common_page_header(page, bpttd_p),
		.level = read_uint32(bplus_tree_page_header_serial, BYTES_FOR_PAGE_LEVEL),
	};
}

void serialize_bplus_tree_page_header(void* hdr_serial, const bplus_tree_page_header* bptph_p, const bplus_tree_tuple_defs* bpttd_p)
{
	serialize_common_page_header(hdr_serial, &(bptph_p->parent), bpttd_p);

	void* bplus_tree_page_header_serial = hdr_serial + get_offset_of_bplus_tree_page_header(bpttd_p);
	write_uint32(bplus_tree_page_header_serial, BYTES_FOR_PAGE_LEVEL, bptph_p->level);
}

void set_bplus_tree_page_header(persistent_page ppage, const bplus_tree_page_header* bptph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p)
{
	uint32_t page_header_size = get_page_header_size(ppage.page, bpttd_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua(ppage.page, bpttd_p->page_size), page_header_size);

	// serialize bptph_p on the hdr_serial
	serialize_bplus_tree_page_header(hdr_serial, bptph_p, bpttd_p);

	// write hdr_serial to the new header position
	pmm_p->set_page_header(pmm_p->context, ppage, bpttd_p->page_size, hdr_serial);

	free(hdr_serial);
}

void print_bplus_tree_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_common_page_header(page, bpttd_p);
	printf("level : %"PRIu32"\n", get_level_of_bplus_tree_page(page, bpttd_p));
}