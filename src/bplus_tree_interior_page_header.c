#include<bplus_tree_interior_page_header.h>

#include<page_layout.h>
#include<int_accesses.h>

#include<stdlib.h>

// number of bytes in the flags field of the header
#define FLAGS_BYTE_SIZE 1

// bit position of is_last_page_of_level
#define IS_LAST_PAGE_OF_LEVEL_FLAG_POS 0

uint32_t get_offset_to_end_of_bplus_interior_leaf_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_bplus_tree_page_header(bpttd_p) + bpttd_p->page_id_width + 1;
}

uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_interior_page_header(page, bpttd_p).least_keys_page_id;
}

int is_last_page_of_level_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_interior_page_header(page, bpttd_p).is_last_page_of_level;
}

static inline uint32_t get_offset_to_bplus_tree_interior_page_header_locals(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_bplus_tree_page_header(bpttd_p);
}

bplus_tree_interior_page_header get_bplus_tree_interior_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* interior_page_header_serial = get_page_header_ua((void*)page, bpttd_p->page_size) + get_offset_to_bplus_tree_interior_page_header_locals(bpttd_p);
	return (bplus_tree_interior_page_header){
		.parent = get_bplus_tree_page_header(page, bpttd_p),
		.least_keys_page_id = read_uint64(interior_page_header_serial, bpttd_p->page_id_width),
		.is_last_page_of_level = ((read_int8(interior_page_header_serial + bpttd_p->page_id_width, FLAGS_BYTE_SIZE) >> IS_LAST_PAGE_OF_LEVEL_FLAG_POS) & 1),
	};
}

void serialize_bplus_tree_interior_page_header(void* hdr_serial, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p)
{
	serialize_bplus_tree_page_header(hdr_serial, &(bptiph_p->parent), bpttd_p);

	void* bplus_tree_interior_page_header_serial = hdr_serial + get_offset_to_bplus_tree_interior_page_header_locals(bpttd_p);
	write_uint64(bplus_tree_interior_page_header_serial, bpttd_p->page_id_width, bptiph_p->least_keys_page_id);
	write_uint64(bplus_tree_interior_page_header_serial + bpttd_p->page_id_width, FLAGS_BYTE_SIZE, (bptiph_p->is_last_page_of_level << IS_LAST_PAGE_OF_LEVEL_FLAG_POS));
}

void set_bplus_tree_interior_page_header(persistent_page ppage, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p)
{
	uint32_t page_header_size = get_page_header_size(ppage.page, bpttd_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua(ppage.page, bpttd_p->page_size), page_header_size);

	// serialize bptlph_p on the hdr_serial
	serialize_bplus_tree_interior_page_header(hdr_serial, bptiph_p, bpttd_p);

	// write hdr_serial to the new header position
	pmm_p->set_page_header(pmm_p->context, ppage, bpttd_p->page_size, hdr_serial);

	free(hdr_serial);
}

#include<stdio.h>

void print_bplus_tree_interior_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_page_header(page, bpttd_p);
	printf("least_keys_page_id : %"PRIu64"\n", get_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p));
	printf("is_last_page_of_level : %d\n", is_last_page_of_level_of_bplus_tree_interior_page(page, bpttd_p));
}