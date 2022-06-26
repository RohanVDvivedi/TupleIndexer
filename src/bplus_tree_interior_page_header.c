#include<bplus_tree_interior_page_header.h>

#include<page_layout.h>
#include<int_accesses.h>

uint32_t sizeof_INTERIOR_PAGE_HEADER(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_size_of_page_type_header() + get_size_of_bplus_tree_page_level_header() + bpttd_p->page_id_width * 2;
}

uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* page_header = get_page_header((void*)page, bpttd_p->page_size);
	const void* leaf_page_header = page_header + get_size_of_page_type_header() + get_size_of_bplus_tree_page_level_header();
	const void* least_keys_page_id = leaf_page_header + 0;
	return read_uint64(least_keys_page_id, bpttd_p->page_id_width);
}

void set_least_keys_page_id_of_bplus_tree_interior_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	void* page_header = get_page_header((void*)page, bpttd_p->page_size);
	void* leaf_page_header = page_header + get_size_of_page_type_header() + get_size_of_bplus_tree_page_level_header();
	void* least_keys_page_id = leaf_page_header + 0;
	return write_uint64(least_keys_page_id, bpttd_p->page_id_width, page_id);
}

uint64_t get_next_page_id_of_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* page_header = get_page_header((void*)page, bpttd_p->page_size);
	const void* leaf_page_header = page_header + get_size_of_page_type_header() + get_size_of_bplus_tree_page_level_header();
	const void* next_page_id = leaf_page_header + bpttd_p->page_id_width;
	return read_uint64(next_page_id, bpttd_p->page_id_width);
}

void set_next_page_id_of_bplus_tree_interior_page(void* page, uint64_t page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	void* page_header = get_page_header((void*)page, bpttd_p->page_size);
	void* leaf_page_header = page_header + get_size_of_page_type_header() + get_size_of_bplus_tree_page_level_header();
	void* next_page_id = leaf_page_header + bpttd_p->page_id_width;
	return write_uint64(next_page_id, bpttd_p->page_id_width, page_id);
}

#include<stdio.h>

void print_bplus_tree_interior_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_page_header(page, bpttd_p->page_size);
	printf("next_page_id : %llu\n", (unsigned long long int)get_next_page_id_of_bplus_tree_interior_page(page, bpttd_p));
	printf("least_keys_page_id : %llu\n", (unsigned long long int)get_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p));
}