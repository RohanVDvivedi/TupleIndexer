#include<bplus_tree_interior_page_header.h>

#include<page_layout.h>
#include<int_accesses.h>

// number of bytes in the flags field of the header
#define FLAGS_BYTE_SIZE 1

// bit position of is_last_page_of_level
#define IS_LAST_PAGE_OF_LEVEL_FLAG_POS 0

static uint32_t get_offset_of_bplus_tree_interior_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_of_bplus_tree_page_level_header(bpttd_p) + get_size_of_bplus_tree_page_level_header();
}

static uint32_t get_size_of_bplus_tree_interior_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return bpttd_p->page_id_width + FLAGS_BYTE_SIZE;
}

uint32_t sizeof_INTERIOR_PAGE_HEADER(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_of_bplus_tree_interior_page_header(bpttd_p) + get_size_of_bplus_tree_interior_page_header(bpttd_p);
}

uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_interior_page_header(bpttd_p);
	const void* least_keys_page_id = leaf_page_header + 0;
	return read_uint64(least_keys_page_id, bpttd_p->page_id_width);
}

void set_least_keys_page_id_of_bplus_tree_interior_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_interior_page_header(bpttd_p);
	void* least_keys_page_id = leaf_page_header + 0;
	return write_uint64(least_keys_page_id, bpttd_p->page_id_width, page_id);
}

int is_last_page_of_level_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_interior_page_header(bpttd_p);
	const void* flags = leaf_page_header + bpttd_p->page_id_width;
	int64_t flags_val = read_int64(flags, FLAGS_BYTE_SIZE);
	return (flags_val >> IS_LAST_PAGE_OF_LEVEL_FLAG_POS) & INT64_C(1);
}

void set_is_last_page_of_level_of_bplus_tree_interior_page(void* page, int is_last_page_of_level, const bplus_tree_tuple_defs* bpttd_p)
{
	void* leaf_page_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_bplus_tree_interior_page_header(bpttd_p);
	void* flags = leaf_page_header + bpttd_p->page_id_width;
	int64_t flags_val = read_int64(flags, FLAGS_BYTE_SIZE);
	if(is_last_page_of_level)
		flags_val |= (INT64_C(1) << IS_LAST_PAGE_OF_LEVEL_FLAG_POS);
	else
		flags_val &= (~(INT64_C(1) << IS_LAST_PAGE_OF_LEVEL_FLAG_POS));
	return write_int64(flags, FLAGS_BYTE_SIZE, flags_val);
}

#include<stdio.h>

void print_bplus_tree_interior_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_page_header(page, bpttd_p);
	printf("least_keys_page_id : %"PRIu64"\n", get_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p));
	printf("is_last_page_of_level : %d\n", is_last_page_of_level_of_bplus_tree_interior_page(page, bpttd_p));
}