#include<tupleindexer/utils/persistent_page.h>

#include<tuplestore/page_layout_unaltered.h>

const void* get_page_header_ua_persistent_page(const persistent_page* ppage, uint32_t page_size)
{
	return get_page_header_ua(ppage->page, page_size);
}

uint32_t get_page_header_size_persistent_page(const persistent_page* ppage, uint32_t page_size)
{
	return get_page_header_size(ppage->page, page_size);
}

uint32_t get_space_required_for_persistent_page_header(uint32_t page_header_size, uint32_t page_size)
{
	return get_space_required_for_page_header(page_header_size, page_size);
}

int can_page_header_fit_on_persistent_page(uint32_t page_header_size, uint32_t page_size)
{
	return can_page_header_fit_on_page(page_header_size, page_size);
}

uint32_t get_maximum_tuple_count_on_persistent_page(uint32_t page_header_size, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_maximum_tuple_count_on_page(page_header_size, page_size, tpl_sz_d);
}

uint32_t get_maximum_tuple_size_accomodatable_on_persistent_page(uint32_t page_header_size, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_maximum_tuple_size_accomodatable_on_page(page_header_size, page_size, tpl_sz_d);
}

uint32_t get_tuple_count_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_tuple_count_on_page(ppage->page, page_size, tpl_sz_d);
}

uint32_t get_tomb_stone_count_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_tomb_stone_count_on_page(ppage->page, page_size, tpl_sz_d);
}

int can_append_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple)
{
	return can_append_tuple_on_page(ppage->page, page_size, tpl_sz_d, external_tuple);
}

int can_insert_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	return can_insert_tuple_on_page(ppage->page, page_size, tpl_sz_d, index, external_tuple);
}

int can_update_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	return can_update_tuple_on_page(ppage->page, page_size, tpl_sz_d, index, external_tuple);
}

int exists_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index)
{
	return exists_tuple_on_page(ppage->page, page_size, tpl_sz_d, index);
}

const void* get_nth_tuple_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index)
{
	return get_nth_tuple_on_page(ppage->page, page_size, tpl_sz_d, index);
}

uint32_t get_free_space_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_free_space_on_page(ppage->page, page_size, tpl_sz_d);
}

uint32_t get_space_occupied_by_tuples_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t start_index, uint32_t last_index)
{
	return get_space_occupied_by_tuples_on_page(ppage->page, page_size, tpl_sz_d, start_index, last_index);
}

uint32_t get_space_occupied_by_all_tuples_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_space_occupied_by_all_tuples_on_page(ppage->page, page_size, tpl_sz_d);
}

uint32_t get_space_occupied_by_all_tomb_stones_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_space_occupied_by_all_tomb_stones_on_page(ppage->page, page_size, tpl_sz_d);
}

uint32_t get_space_to_be_occupied_by_tuple_on_persistent_page(uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple)
{
	return get_space_to_be_occupied_by_tuple_on_page(page_size, tpl_sz_d, external_tuple);
}

uint32_t get_space_allotted_to_all_tuples_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_space_allotted_to_all_tuples_on_page(ppage->page, page_size, tpl_sz_d);
}

uint32_t get_space_to_be_allotted_to_all_tuples_on_persistent_page(uint32_t page_header_size, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_space_to_be_allotted_to_all_tuples_on_page(page_header_size, page_size, tpl_sz_d);
}

uint32_t get_fragmentation_space_on_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_fragmentation_space_on_page(ppage->page, page_size, tpl_sz_d);
}

uint32_t get_additional_space_overhead_per_tuple_on_persistent_page(uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	return get_additional_space_overhead_per_tuple_on_page(page_size, tpl_sz_d);
}

void print_persistent_page(const persistent_page* ppage, uint32_t page_size, const tuple_def* tpl_d)
{
	print_page(ppage->page, page_size, tpl_d);
}