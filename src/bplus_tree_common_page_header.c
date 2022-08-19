#include<bplus_tree_common_page_header.h>

#include<common_page_header.h>

#include<page_layout.h>
#include<int_accesses.h>

// number of bytes to store level of the page
// 2 is more than enough for curent computer systems, and allows us to store large amount of data
// values in range 1 to 4 both inclusive
#define BYTES_FOR_PAGE_LEVEL 2

uint32_t get_offset_of_bplus_tree_page_level_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_of_page_type_header(bpttd_p) + get_size_of_page_type_header();
}

uint32_t get_size_of_bplus_tree_page_level_header()
{
	return BYTES_FOR_PAGE_LEVEL;
}

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

void print_bplus_tree_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_common_page_header(page, bpttd_p);
	printf("level : %"PRIu32"\n", get_level_of_bplus_tree_page(page, bpttd_p));
}

int is_bplus_tree_leaf_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_level_of_bplus_tree_page(page, bpttd_p) == 0;
}