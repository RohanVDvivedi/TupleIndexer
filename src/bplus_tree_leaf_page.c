#include<bplus_tree_leaf_page.h>

#include<page_layout.h>

uint32_t get_next_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size)
{
	const bplus_tree_leaf_page_header* ph = get_page_header((void*)page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	return ph->next_page;
}

uint32_t get_prev_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size)
{
	const bplus_tree_leaf_page_header* ph = get_page_header((void*)page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	return ph->prev_page;
}