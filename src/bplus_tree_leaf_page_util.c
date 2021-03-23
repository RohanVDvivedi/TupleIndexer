#include<bplus_tree_leaf_page_util.h>

#include<page_layout.h>

int is_leaf_page(const void* page)
{
	return get_page_type(page) == LEAF_PAGE_TYPE;
}

uint32_t get_next_leaf_page(const void* page)
{
	return get_reference_page_id(page, NEXT_PAGE_REF);
}

uint32_t get_prev_leaf_page(const void* page)
{
	return get_reference_page_id(page, PREV_PAGE_REF);
}

uint16_t get_record_count_in_leaf_page(const void* page)
{
	return get_tuple_count(page);
}

const void* get_record_from_leaf_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds)
{
	return get_nth_tuple(page, page_size, bpttds->record_def, index);
}

uint16_t find_in_leaf_page(const void* page, uint32_t page_size, const void* like_key, const bplus_tree_tuple_defs* bpttds);
