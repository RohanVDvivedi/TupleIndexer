#include<bplus_tree_page.h>

#include<common_page_header.h>

#include<page_layout.h>
#include<int_accesses.h>

// number of bytes to store level of the page
// 2 is more than enough for curent computer systems, and allows us to store large amount of data
// values in range 1 to 4 both inclusive
#define BYTES_FOR_PAGE_LEVEL 2

uint32_t get_size_of_bplus_tree_page_level_header()
{
	return BYTES_FOR_PAGE_LEVEL;
}

uint32_t get_level_of_bplus_tree_page(const void* page, uint32_t page_size)
{
	const void* page_header = get_page_header((void*)page, page_size);
	const void* page_level_header = page_header + get_size_of_page_type_header();
	return read_uint32(page_level_header, BYTES_FOR_PAGE_LEVEL);
}

void set_level_of_bplus_tree_page(void* page, uint32_t page_size, uint32_t level)
{
	void* page_header = get_page_header((void*)page, page_size);
	void* page_level_header = page_header + get_size_of_page_type_header();
	write_uint32(page_level_header, BYTES_FOR_PAGE_LEVEL, level);
}

void print_bplus_tree_page_header(const void* page, uint32_t page_size)
{
	print_common_page_header(page, page_size);
	printf("level : %u\n", get_level_of_bplus_tree_page(page, page_size));
}

int is_bplus_tree_leaf_page(const void* page, uint32_t page_size)
{
	return get_level_of_bplus_tree_page(page, page_size) == 0;
}