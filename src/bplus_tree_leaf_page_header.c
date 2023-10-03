#include<bplus_tree_leaf_page_header.h>

#include<page_layout.h>
#include<int_accesses.h>

static uint32_t get_offset_of_bplus_tree_leaf_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_of_bplus_tree_page_level_header(bpttd_p) + get_size_of_bplus_tree_page_level_header();
}

static uint32_t get_size_of_bplus_tree_leaf_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return bpttd_p->page_id_width * 2;
}

uint32_t sizeof_LEAF_PAGE_HEADER(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_of_bplus_tree_leaf_page_header(bpttd_p) + get_size_of_bplus_tree_leaf_page_header(bpttd_p);
}

uint64_t get_next_page_id_of_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_leaf_page_header(bpttd_p);
	const void* next_page_id = leaf_page_header + 0;
	return read_uint64(next_page_id, bpttd_p->page_id_width);
}

void set_next_page_id_of_bplus_tree_leaf_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_leaf_page_header(bpttd_p);
	void* next_page_id = leaf_page_header + 0;
	return write_uint64(next_page_id, bpttd_p->page_id_width, page_id);
}

uint64_t get_prev_page_id_of_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_leaf_page_header(bpttd_p);
	const void* prev_page_id = leaf_page_header + bpttd_p->page_id_width;
	return read_uint64(prev_page_id, bpttd_p->page_id_width);
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

bplus_tree_leaf_page_header get_bplus_tree_leaf_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p);

void serialize_bplus_tree_leaf_page_header(void* hdr_serial, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p);

void set_bplus_tree_leaf_page_header(persistent_page ppage, const bplus_tree_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p);

#include<stdio.h>

void print_bplus_tree_leaf_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_page_header(page, bpttd_p);
	printf("next_page_id : %"PRIu64"\n", get_next_page_id_of_bplus_tree_leaf_page(page, bpttd_p));
	printf("prev_page_id : %"PRIu64"\n", get_prev_page_id_of_bplus_tree_leaf_page(page, bpttd_p));
}