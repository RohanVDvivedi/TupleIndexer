#include<page_table_page_util.h>

#include<page_table_page_header.h>
#include<persistent_page_functions.h>

#include<stdlib.h>

int init_page_table_page(persistent_page* ppage, uint32_t level, uint64_t first_bucket_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, pttd_p->pas_p->page_size, sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), &(pttd_p->entry_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	page_table_page_header hdr = get_page_table_page_header(ppage, pttd_p);
	hdr.parent.type = PAGE_TABLE_PAGE;
	hdr.level = level;
	hdr.first_bucket_id = first_bucket_id;
	set_page_table_page_header(ppage, &hdr, pttd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}

void print_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	print_page_table_page_header(ppage, pttd_p);
	print_persistent_page(ppage, pttd_p->pas_p->page_size, pttd_p->entry_def);
}

uint64_t get_child_page_id_at_child_index_in_page_table_page(const persistent_page* ppage, uint32_t child_index, const page_table_tuple_defs* pttd_p)
{
	// child_index out of range
	if(child_index >= pttd_p->entries_per_page)
		return pttd_p->pas_p->NULL_PAGE_ID;

	const void* child_tuple = get_nth_tuple_on_persistent_page(ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def), child_index);

	// if the child_tuple is NULL, then it is NULL_PAGE_ID
	if(child_tuple == NULL)
		return pttd_p->pas_p->NULL_PAGE_ID;

	// the tuple has only 1 non-NULLable UINT value, hence we can directly access it
	return get_value_from_element_from_tuple(pttd_p->entry_def, 0, child_tuple).uint_value;
}

int set_child_page_id_at_child_index_in_page_table_page(persistent_page* ppage, uint32_t child_index, uint64_t child_page_id, const page_table_tuple_defs* pttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// child_index out of range
	if(child_index >= pttd_p->entries_per_page)
		return 0;

	if(child_page_id == pttd_p->pas_p->NULL_PAGE_ID)
	{
		// we need to set NULL at child_index
		// we do not need to use the update_resiliently, because entry_def is fixed width tuple with only 1 element
		// if the child_index is greater than or equal to the tuple_count on the page, then this function fails and we do not need to take care of it, since the tuple still get read as NULL by the getter function above
		update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def), child_index, NULL, abort_error);
		if(*abort_error)
			return 0;
	}
	else
	{
		// keep on appending NULLs, until child_index is not in bounds of the tuple_count on the page
		while(child_index >= get_tuple_count_on_persistent_page(ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def)))
		{
			append_tuple_on_persistent_page(pmm_p, transaction_id, ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def), NULL, abort_error);
			if(*abort_error)
				return 0;
		}

		// now since the child_index is within bounds of tuple_count, we can directly update

		// construct a tuple in temporary memory and make it point to child_page_id
		void* new_child_tuple = malloc(get_minimum_tuple_size(pttd_p->entry_def)); // minimum size of fixed width tuple is same as its size
		init_tuple(pttd_p->entry_def, new_child_tuple);
		set_element_in_tuple(pttd_p->entry_def, 0, new_child_tuple, &((user_value){.uint_value = child_page_id}));

		// perform update
		update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def), child_index, new_child_tuple, abort_error);
		free(new_child_tuple);
		if(*abort_error)
			return 0;
	}

	return 1;
}

int has_all_NULL_PAGE_ID_in_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	// if tomb_stone_count == tuple_count, then this means all child_page_id's are NULL_PAGE_ID
	return get_tuple_count_on_persistent_page(ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def)) == get_tomb_stone_count_on_persistent_page(ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def));
}

uint32_t get_non_NULL_PAGE_ID_count_in_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	// this is simply equal to tuple_count - tomb_stone_count
	return get_tuple_count_on_persistent_page(ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def)) - get_tomb_stone_count_on_persistent_page(ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def));
}
