#include<bplus_tree_interior_page.h>

#include<page_layout.h>

uint32_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, uint32_t page_size)
{
	const bplus_tree_interior_page_header* ph = get_page_header((void*)page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	return ph->least_keys_page_id;
}

void set_least_keys_page_id_of_bplus_tree_interior_page(void* page, uint32_t page_size, uint32_t page_id)
{
	bplus_tree_interior_page_header* ph = get_page_header(page, page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	ph->least_keys_page_id = page_id;
}