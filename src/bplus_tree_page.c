#include<bplus_tree_page.h>

#include<common_page_header.h>

#include<page_layout.h>

uint32_t get_level_of_bplus_tree_page(const void* page, uint32_t page_size)
{
	const bplus_tree_page_header* ph = get_page_header((void*)page, page_size) + sizeof(page_header);
	return ph->level;
}