#include<bplus_tree_interior_page_util.h>

#include<tuple.h>
#include<page_layout.h>

int is_interior_page(const void* page)
{
	return get_page_type(page) == INTERIOR_PAGE_TYPE;
}

uint16_t get_index_entry_count_in_interior_page(const void* page)
{
	return get_tuple_count(page);
}

uint32_t get_index_page_id_from_interior_page(const void* page, uint32_t page_size, int32_t index, const bplus_tree_tuple_defs* bpttds)
{
	if(index == -1)
		return get_reference_page_id(page, ALL_LEAST_REF);

	// if index is out of bounds
	if(index >= get_tuple_count(page) || index < -1)
		return 0;

	const void* index_tuple = get_nth_tuple(page, page_size, bpttds->index_def, index);
	return *(get_element(bpttds->index_def, bpttds->index_def->element_count - 1, index_tuple).UINT_4);
}

const void* get_index_entry_from_interior_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds)
{
	return get_nth_tuple(page, page_size, bpttds->index_def, index);
}

uint32_t find_in_interior_page(const void* page, const void* like_key, const bplus_tree_tuple_defs* bpttds);

const void* insert_or_split_interior_page(const void* intr_page, const void* new_index_tuple, const bplus_tree_tuple_defs* bpttds);

const void* merge_child_pages_at(const void* intr_page, uint16_t index, const bplus_tree_tuple_defs* bpttds);
