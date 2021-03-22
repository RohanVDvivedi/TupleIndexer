#include<bplus_tree_interior_page_util.h>

#include<page_layout.h>

int is_interior_page(const void* page)
{
	return get_page_type(page) == INTERIOR_PAGE_TYPE;
}

uint16_t get_index_entry_count_in_interior_page(const void* page)
{
	return get_tuple_count(page);
}

const void* get_index_entry_from_interior_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds)
{
	return get_nth_tuple(page, page_size, bpttds->index_def, index);
}

uint32_t find_in_interior_page(const void* page, const void* like_key, const bplus_tree_tuple_defs* bpttds);

const void* insert_or_split_interior_page(const void* intr_page, const void* new_index_tuple, const bplus_tree_tuple_defs* bpttds);

const void* merge_child_pages_at(const void* intr_page, uint16_t index, const bplus_tree_tuple_defs* bpttds);
