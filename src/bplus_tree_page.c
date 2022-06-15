#include<bplus_tree_page.h>

#include<common_page_header.h>

#include<page_layout.h>

uint32_t get_level_of_bplus_tree_page(const void* page, uint32_t page_size)
{
	const bplus_tree_page_header* ph = get_page_header((void*)page, page_size) + sizeof(page_header);
	return ph->level;
}

void set_level_of_bplus_tree_page(void* page, uint32_t page_size, uint32_t level)
{
	bplus_tree_page_header* ph = get_page_header(page, page_size) + sizeof(page_header);
	ph->level = level;
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