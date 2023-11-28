#include<page_table_page_util.h>

#include<page_table_page_header.h>
#include<persistent_page_functions.h>
#include<invalid_tuple_indices.h>

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

uint64_t get_first_bucket_id_for_level_containing_bucket_id_for_page_table_page(uint32_t level, uint64_t bucket_id, const page_table_tuple_defs* pttd_p)
{
	uint64_t bucket_range_size;

	// if the bucket_range_size is greater than (entries_per_page^(level+1)), then all the bucket_ids point to first_bucket_id
	if(0 == get_power_of_entries_per_page(pttd_p, level + 1, &bucket_range_size))
		return 0;

	return (bucket_id / bucket_range_size) * bucket_range_size;
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

page_table_bucket_range get_bucket_range_for_page_table_page(const persistent_page* ppage, const page_table_tuple_defs* pttd_p)
{
	page_table_page_header hdr = get_page_table_page_header(ppage, pttd_p);

	page_table_bucket_range result = {.first_bucket_id = hdr.first_bucket_id};

	uint64_t bucket_range_size;
	if(0 == get_power_of_entries_per_page(pttd_p, hdr.level + 1, &bucket_range_size))
		goto EXIT_OVERFLOW;

	if(will_unsigned_sum_overflow(uint64_t, result.first_bucket_id, (bucket_range_size-1)))
		goto EXIT_OVERFLOW;

	result.last_bucket_id = result.first_bucket_id + (bucket_range_size-1);

	// effectively equal to [first_bucket_id, first_bucket_id + (entries_per_page ^ (level + 1)) - 1]
	return result;

	EXIT_OVERFLOW:;
	result.last_bucket_id = UINT64_MAX;
	return result;
}

page_table_bucket_range get_delegated_bucket_range_for_child_index_on_page_table_page(const persistent_page* ppage, uint32_t child_index, const page_table_tuple_defs* pttd_p)
{
	// child_index must be within bounds of entries_per_page
	if(child_index >= pttd_p->entries_per_page)
		goto EXIT_OUT_OF_BOUNDS_CHILD;

	page_table_page_header hdr = get_page_table_page_header(ppage, pttd_p);

	page_table_bucket_range result;

	uint64_t child_bucket_range_size;
	if(0 == get_power_of_entries_per_page(pttd_p, hdr.level, &child_bucket_range_size))
	{
		if(child_index != 0)
			goto EXIT_OUT_OF_BOUNDS_CHILD;
		else
		{
			// if the child_index is 0, then the first_bucket_id will not overflow but the last_bucket_id will
			result = (page_table_bucket_range){.first_bucket_id = hdr.first_bucket_id};
			goto EXIT_OVERFLOW;
		}
	}

	if(will_unsigned_mul_overflow(uint64_t, child_bucket_range_size, child_index))
		goto EXIT_OUT_OF_BOUNDS_CHILD;
	if(will_unsigned_sum_overflow(uint64_t, hdr.first_bucket_id, child_bucket_range_size * child_index))
		goto EXIT_OUT_OF_BOUNDS_CHILD;

	result = (page_table_bucket_range){.first_bucket_id = (hdr.first_bucket_id + child_bucket_range_size * child_index)};

	if(will_unsigned_sum_overflow(uint64_t, result.first_bucket_id, (child_bucket_range_size-1)))
		goto EXIT_OVERFLOW;

	result.last_bucket_id = result.first_bucket_id + (child_bucket_range_size-1);

	// effectively equal to [first_bucket_id + child_index * (entries_per_page ^ level), first_bucket_id + (child_index) * (entries_per_page ^ level) + (entries_per_page ^ level) - 1]
	return result;

	EXIT_OVERFLOW:;
	result.last_bucket_id = UINT64_MAX;
	return result;

	// this is what happens when even the first_bucket_id of the child's delegated range is out of 64 bit number
	EXIT_OUT_OF_BOUNDS_CHILD:
	return (page_table_bucket_range){.first_bucket_id = UINT64_MAX, .last_bucket_id = 0};
}

uint32_t get_child_index_for_bucket_id_on_page_table_page(const persistent_page* ppage, uint64_t bucket_id, const page_table_tuple_defs* pttd_p)
{
	page_table_bucket_range bucket_range_for_page = get_bucket_range_for_page_table_page(ppage, pttd_p);

	if(!is_bucket_contained_page_table_bucket_range(&bucket_range_for_page, bucket_id))
		return NO_TUPLE_FOUND;

	page_table_page_header hdr = get_page_table_page_header(ppage, pttd_p);

	uint64_t child_bucket_range_size;
	if(0 == get_power_of_entries_per_page(pttd_p, hdr.level, &child_bucket_range_size)) // if child_bucket_range_size is too big then, you can only access the first child
		return 0;

	// effectively equal to (bucket_id - first_bucket_id) / (entries_per_page ^ level)
	return (bucket_id - bucket_range_for_page.first_bucket_id) / child_bucket_range_size;
}

int level_up_page_table_page(persistent_page* ppage, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// grab the header of the page
	page_table_page_header hdr = get_page_table_page_header(ppage, pttd_p);

	// cache the old header value
	const page_table_page_header old_hdr = hdr;
	uint64_t only_child_page_id = pttd_p->pas_p->NULL_PAGE_ID;

	// if the ppage has atleast 1 non-NULL_PAGE_ID, then its contents have to preserved in its child
	if(!has_all_NULL_PAGE_ID_in_page_table_page(ppage, pttd_p))
	{
		// we do not need to create a new child page, if the ppage has only 1 child
		if(get_non_NULL_PAGE_ID_count_in_page_table_page(ppage, pttd_p) == 1)
		{
			// find the only child (only_child_page_id) of this ppage
			for(uint32_t i = 0; i < pttd_p->entries_per_page; i++)
			{
				if(get_child_page_id_at_child_index_in_page_table_page(ppage, i, pttd_p) != pttd_p->pas_p->NULL_PAGE_ID)
				{
					only_child_page_id = get_child_page_id_at_child_index_in_page_table_page(ppage, i, pttd_p);
					break;
				}
			}
		}
		else // else create a new child page that will contain all the children of ppage
		{
			// get lock on a new child page
			persistent_page only_child_page = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);
			if(*abort_error)
				return 0;

			// clone contents of the ppage onto the only_child_page
			clone_persistent_page(pmm_p, transaction_id, &only_child_page, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def), ppage, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, NONE_OPTION, abort_error);
				return 0;
			}

			// now we are done with only_child_page, release lock on it
			only_child_page_id = only_child_page.page_id;
			release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, NONE_OPTION, abort_error);
			if(*abort_error)
				return 0;
		}
	}

	// discard all tuples from the ppage
	discard_all_tuples_on_persistent_page(pmm_p, transaction_id, ppage, pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def), abort_error);

	// increment its level and update first_bucket_id
	hdr.level = old_hdr.level + 1;
	hdr.first_bucket_id = get_first_bucket_id_for_level_containing_bucket_id_for_page_table_page(old_hdr.level + 1, old_hdr.first_bucket_id, pttd_p);

	// write new header hdr onto the ppage
	set_page_table_page_header(ppage, &hdr, pttd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// if we needed to copy contents of the ppage onto its child, then make an entry for it on this promoted ppage
	if(only_child_page_id != pttd_p->pas_p->NULL_PAGE_ID)
	{
		// the entry goes for only_child_page.first_bucket_id -> only_child_page_id
		uint32_t child_index_for_only_child_page = get_child_index_for_bucket_id_on_page_table_page(ppage, old_hdr.first_bucket_id, pttd_p);
		set_child_page_id_at_child_index_in_page_table_page(ppage, child_index_for_only_child_page, only_child_page_id, pttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	return 1;
}

int level_down_page_table_page(persistent_page* ppage, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// if the page is leaf OR if the count of non-NULL_PAGE_ID entry is not 1, then it can can not be levelled down
	if(is_page_table_leaf_page(ppage, pttd_p) || get_non_NULL_PAGE_ID_count_in_page_table_page(ppage, pttd_p) != 1)
		return 0;

	// find the only child (only_child_page_id) of this ppage
	uint64_t only_child_page_id = pttd_p->pas_p->NULL_PAGE_ID;
	for(uint32_t i = 0; i < pttd_p->entries_per_page; i++)
	{
		if(get_child_page_id_at_child_index_in_page_table_page(ppage, i, pttd_p) != pttd_p->pas_p->NULL_PAGE_ID)
		{
			only_child_page_id = get_child_page_id_at_child_index_in_page_table_page(ppage, i, pttd_p);
			break;
		}
	}

	// get write lock on this only_child_page
	persistent_page only_child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, only_child_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		return 0;

	// clone the only_child_page onto the ppage
	clone_persistent_page(pmm_p, transaction_id, ppage,  pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def), &only_child_page, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, NONE_OPTION, abort_error);
		return 0;
	}

	// free the only_child_page
	release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, FREE_PAGE, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, NONE_OPTION, abort_error);
		return 0;
	}

	return 1;
}