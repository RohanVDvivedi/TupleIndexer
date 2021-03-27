#include<bplus_tree_leaf_page_util.h>

#include<tuple.h>
#include<page_layout.h>

#include<sorted_packed_page_util.h>

int is_leaf_page(const void* page)
{
	return get_page_type(page) == LEAF_PAGE_TYPE;
}

int init_leaf_page(void* page, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	return init_page(page, page_size, LEAF_PAGE_TYPE, 2, bpttds->record_def);
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
