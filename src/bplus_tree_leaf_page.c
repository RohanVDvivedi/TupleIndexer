#include<bplus_tree_leaf_page.h>

#include<page_layout.h>

uint32_t get_next_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size)
{
	const bplus_tree_leaf_page_header* ph = get_page_header((void*)page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	return ph->next_page_id;
}

void set_next_page_id_of_bplus_tree_leaf_page(void* page, uint32_t page_size, uint32_t page_id)
{
	bplus_tree_leaf_page_header* ph = get_page_header(page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	ph->next_page_id = page_id;
}

uint32_t get_prev_page_id_of_bplus_tree_leaf_page(const void* page, uint32_t page_size)
{
	const bplus_tree_leaf_page_header* ph = get_page_header((void*)page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	return ph->prev_page_id;
}

void set_prev_page_id_of_bplus_tree_leaf_page(void* page, uint32_t page_size, uint32_t page_id)
{
	bplus_tree_leaf_page_header* ph = get_page_header(page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	ph->prev_page_id = page_id;
}