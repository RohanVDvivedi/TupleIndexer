#include<bplus_tree_leaf_page_header.h>

#include<page_layout_unaltered.h>
#include<int_accesses.h>

#include<stdlib.h>

void set_next_page_id_of_bplus_tree_leaf_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_leaf_page_header(bpttd_p);
	void* next_page_id = leaf_page_header + 0;
	return write_uint64(next_page_id, bpttd_p->page_id_width, page_id);
}

void set_prev_page_id_of_bplus_tree_leaf_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_leaf_page_header(bpttd_p);
	void* prev_page_id = leaf_page_header + bpttd_p->page_id_width;
	return write_uint64(prev_page_id, bpttd_p->page_id_width, page_id);
}

// --

uint32_t get_offset_to_end_of_bplus_tree_leaf_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_bplus_tree_page_header(bpttd_p) + (2 * bpttd_p->page_id_width);
}

uint64_t get_next_page_id_of_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_leaf_page_header(page, bpttd_p).next_page_id;
}

uint64_t get_prev_page_id_of_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_leaf_page_header(page, bpttd_p).prev_page_id;
}

static inline uint32_t get_offset_to_bplus_tree_leaf_page_header_locals(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_bplus_tree_page_header(bpttd_p);
}

bplus_tree_leaf_page_header get_bplus_tree_leaf_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* leaf_page_header_serial = get_page_header_ua((void*)page, bpttd_p->page_size) + get_offset_to_bplus_tree_leaf_page_header_locals(bpttd_p);
	return (bplus_tree_leaf_page_header){
		.parent = get_bplus_tree_page_header(page, bpttd_p),
		.next_page_id = read_uint64(leaf_page_header_serial, bpttd_p->page_id_width),
		.prev_page_id = read_uint64(leaf_page_header_serial + bpttd_p->page_id_width, bpttd_p->page_id_width),
	};
}

void serialize_bplus_tree_leaf_page_header(void* hdr_serial, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p);

void set_bplus_tree_leaf_page_header(persistent_page ppage, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p)
{
	uint32_t page_header_size = get_page_header_size(ppage.page, bpttd_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua(ppage.page, bpttd_p->page_size), page_header_size);

	// serialize bptlph_p on the hdr_serial
	serialize_bplus_tree_leaf_page_header(hdr_serial, bptlph_p, bpttd_p);

	// write hdr_serial to the new header position
	pmm_p->set_page_header(pmm_p->context, ppage, bpttd_p->page_size, hdr_serial);

	free(hdr_serial);
}

#include<stdio.h>

void print_bplus_tree_leaf_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_page_header(page, bpttd_p);
	printf("next_page_id : %"PRIu64"\n", get_next_page_id_of_bplus_tree_leaf_page(page, bpttd_p));
	printf("prev_page_id : %"PRIu64"\n", get_prev_page_id_of_bplus_tree_leaf_page(page, bpttd_p));
}