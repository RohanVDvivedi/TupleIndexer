#include<bplus_tree_virtual_unsplitted_persistent_page.h>

virtual_unsplitted_persistent_page get_virtual_unsplitted_persistent_page(const persistent_page* ppage, uint32_t page_size, const void* tuple_to_insert, uint32_t* insertion_index, const tuple_def* tpl_d, const uint32_t* key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count)
{
	return (virtual_unsplitted_persistent_page){
		.ppage = ppage,
		.page_size = page_size,
		.tuple_to_insert = tuple_to_insert,
		.insertion_index = insertion_index,
		.tpl_d = tpl_d,
		.key_element_ids = key_element_ids,
		.key_compare_direction = key_compare_direction,
		.key_element_count = key_element_count,
	};
}

uint32_t get_tuple_count_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p)
{
	return get_tuple_count_on_persistent_page(vupp_p->ppage, vupp_p->page_size, vupp_p->tpl_d) + 1;
}

const void* get_nth_tuple_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t index)
{
	if(index < vupp_p->insertion_index)
		return get_nth_tuple_on_persistent_page(vupp_p->ppage, vupp_p->page_size, vupp_p->tpl_d->tpl_sz_d, index);
	else if(index == vupp_p->insertion_index)
		return vupp_p->tuple_to_insert;
	else
		return get_nth_tuple_on_persistent_page(vupp_p->ppage, vupp_p->page_size, vupp_p->tpl_d->tpl_sz_d, index-1);
}

uint32_t get_space_occupied_by_tuples_on_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t start_index, uint32_t last_index)
{
	// invalid parameters, check
	if(start_index > last_index || last_index >= get_tuple_count_on_virtual_unsplitted_persistent_page(vupp_p))
		return 0;

	// cauculate indices for the tuple on the page
	uint32_t start_index_on_ppage = start_index;
	if(start_index > vupp_p->insertion_index)
		start_index_on_ppage--;

	uint32_t last_index_on_ppage = last_index;
	if(last_index >= vupp_p->insertion_index)
		last_index_on_ppage--;

	uint32_t res = 0;

	if(start_index_on_ppage <= last_index_on_ppage && last_index_on_ppage < get_tuple_count_on_persistent_page(vupp_p->ppage, vupp_p->page_size, vupp_p->tpl_d))
		res += get_space_occupied_by_tuples_on_persistent_page(vupp_p->ppage, vupp_p->page_size, vupp_p->tpl_d->tpl_sz_d, start_index_on_ppage, last_index_on_ppage);

	if(start_index <= vupp_p->insertion_index && vupp_p->insertion_index <= last_index)
		res += get_tuple_size(vupp_p->tpl_d, vupp_p->tuple_to_insert) + get_additional_space_overhead_per_tuple_on_persistent_page(vupp_p->page_size, vupp_p->tpl_d->tpl_sz_d);

	return res;
}