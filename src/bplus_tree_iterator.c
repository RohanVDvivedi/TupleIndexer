#include<bplus_tree_iterator.h>

#include<page_layout.h>

#include<stdlib.h>

bplus_tree_iterator* get_new_bplus_tree_iterator(void* curr_page, uint64_t curr_page_id, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// the following 2 must be present
	if(bpttd_p == NULL || dam_p == NULL)
		return NULL;

	if(curr_page_id == bpttd_p->NULL_PAGE_ID)
		return NULL;

	// if the curr_page has not been locked yet then lock it
	if(curr_page == NULL)
	{
		curr_page = dam_p->acquire_page_with_reader_lock(dam_p->context, curr_page_id);
		if(curr_page == NULL)
			return NULL;
	}

	if(curr_tuple_index == LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE)
		curr_tuple_index = get_tuple_count(curr_page, bpttd_p->page_size, bpttd_p->record_def) - 1;

	bplus_tree_iterator* bpi_p = malloc(sizeof(bplus_tree_iterator));
	bpi_p->curr_page = curr_page;
	bpi_p->curr_page_id = curr_page_id;
	bpi_p->curr_tuple_index = curr_tuple_index;
	bpi_p->bpttd_p = bpttd_p;
	bpi_p->dam_p = dam_p;

	return bpi_p;
}

int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	return 0;
}

const void* get_curr_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	if(bpi_p->curr_page == NULL || bpi_p->curr_page_id == bpi_p->bpttd_p->NULL_PAGE_ID || 
		bpi_p->curr_tuple_index >= get_tuple_count(bpi_p->curr_page, bpi_p->bpttd_p->page_size, bpi_p->bpttd_p->record_def))
		return NULL;
	return get_nth_tuple(bpi_p->curr_page, bpi_p->bpttd_p->page_size, bpi_p->bpttd_p->record_def, bpi_p->curr_tuple_index);
}

int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	return 0;
}

void delete_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	if(bpi_p->curr_page_id != bpi_p->bpttd_p->NULL_PAGE_ID && bpi_p->curr_page != NULL)
		bpi_p->dam_p->release_reader_lock_on_page(bpi_p->dam_p->context, bpi_p->curr_page);
	free(bpi_p);
}
