#include<tupleindexer/utils/virtual_unsplitted_persistent_page.h>

virtual_unsplitted_persistent_page get_virtual_unsplitted_persistent_page(const persistent_page* ppage, uint32_t page_size, const void* tuple_to_insert, uint32_t insertion_index, const tuple_def* tpl_d)
{
	return (virtual_unsplitted_persistent_page){
		.ppage = ppage,
		.page_size = page_size,
		.tuple_to_insert = tuple_to_insert,
		.insertion_index = insertion_index,
		.tpl_d = tpl_d,
	};
}

uint32_t get_tuple_count_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p)
{
	return get_tuple_count_on_persistent_page(vupp_p->ppage, vupp_p->page_size, &(vupp_p->tpl_d->size_def)) + 1;
}

const void* get_nth_tuple_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t index)
{
	if(index < vupp_p->insertion_index)
		return get_nth_tuple_on_persistent_page(vupp_p->ppage, vupp_p->page_size, &(vupp_p->tpl_d->size_def), index);
	else if(index == vupp_p->insertion_index)
		return vupp_p->tuple_to_insert;
	else
		return get_nth_tuple_on_persistent_page(vupp_p->ppage, vupp_p->page_size, &(vupp_p->tpl_d->size_def), index-1);
}

uint32_t get_space_occupied_by_tuples_on_virtual_unsplitted_persistent_page(const virtual_unsplitted_persistent_page* vupp_p, uint32_t start_index, uint32_t last_index)
{
	// invalid parameters, check
	if(start_index > last_index || last_index >= get_tuple_count_on_virtual_unsplitted_persistent_page(vupp_p))
		return 0;

	// calculate indices for the tuple on the page
	uint32_t start_index_on_ppage = start_index;
	if(start_index > vupp_p->insertion_index)
		start_index_on_ppage--;

	uint32_t last_index_on_ppage = last_index;
	if(last_index >= vupp_p->insertion_index)
		last_index_on_ppage--;

	uint32_t res = 0;

	// when start_index = insertion_index and last_index = insertion_index, i.e. the caller only request for space_occupied by tuple_to_insert
	// then the start_index_on_ppage to last_index_on_ppage, (the on-page range) range becomes invalid -> resulting in branch to not be taken
	if(start_index_on_ppage <= last_index_on_ppage && last_index_on_ppage < get_tuple_count_on_persistent_page(vupp_p->ppage, vupp_p->page_size, &(vupp_p->tpl_d->size_def)))
		res += get_space_occupied_by_tuples_on_persistent_page(vupp_p->ppage, vupp_p->page_size, &(vupp_p->tpl_d->size_def), start_index_on_ppage, last_index_on_ppage);

	if(start_index <= vupp_p->insertion_index && vupp_p->insertion_index <= last_index)
		res += get_space_to_be_occupied_by_tuple_on_persistent_page(vupp_p->page_size, &(vupp_p->tpl_d->size_def), vupp_p->tuple_to_insert);

	return res;
}