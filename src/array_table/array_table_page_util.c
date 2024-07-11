#include<array_table_page_util.h>

#include<array_table_page_header.h>
#include<persistent_page_functions.h>
#include<invalid_tuple_indices.h>

#include<stdlib.h>

int init_array_table_page(persistent_page* ppage, uint32_t level, uint64_t first_bucket_id, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	if(level == 0)
	{
		int inited = init_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, sizeof_ARRAY_TABLE_PAGE_HEADER(attd_p), &(attd_p->record_def->size_def), abort_error);
		if((*abort_error) || !inited)
			return 0;
	}
	else
	{
		int inited = init_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, sizeof_ARRAY_TABLE_PAGE_HEADER(attd_p), &(attd_p->index_def->size_def), abort_error);
		if((*abort_error) || !inited)
			return 0;
	}

	// get the header, initialize it and set it back on to the page
	array_table_page_header hdr = get_array_table_page_header(ppage, attd_p);
	hdr.parent.type = PAGE_TABLE_PAGE;
	hdr.level = level;
	hdr.first_bucket_id = first_bucket_id;
	set_array_table_page_header(ppage, &hdr, attd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}

uint64_t get_first_bucket_id_for_level_containing_bucket_id_for_array_table_page(uint32_t level, uint64_t bucket_id, const array_table_tuple_defs* attd_p)
{
	uint64_t bucket_range_size;

	// if the bucket_range_size is greater than get_leaf_entries_refrenceable(level+1), then all the bucket_ids point to first_bucket_id
	if(0 == get_leaf_entries_refrenceable_by_entry_at_given_level_using_array_table_tuple_definitions(attd_p, level + 1, &bucket_range_size))
		return 0;

	return (bucket_id / bucket_range_size) * bucket_range_size;
}

void print_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	print_array_table_page_header(ppage, attd_p);
	if(is_array_table_leaf_page(ppage, attd_p))
		print_persistent_page(ppage, attd_p->pas_p->page_size, attd_p->record_def);
	else
		print_persistent_page(ppage, attd_p->pas_p->page_size, attd_p->index_def);
}

int is_NULL_at_child_index_in_array_table_page(const persistent_page* ppage, uint32_t child_index, const array_table_tuple_defs* attd_p)
{
	if(is_array_table_leaf_page(ppage, attd_p))
		return NULL == get_nth_tuple_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def), child_index);
	else
		return NULL == get_nth_tuple_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), child_index);
}

const void* get_record_entry_at_child_index_in_array_table_leaf_page(const persistent_page* ppage, uint32_t child_index, void* preallocated_memory, const array_table_tuple_defs* attd_p)
{
	const void* on_page_record = get_nth_tuple_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def), child_index);

	// if NULL
	if(on_page_record == NULL)
		return NULL;

	// pre allocated memory not provided, then return the on page reference
	if(preallocated_memory == NULL)
		return on_page_record;

	// otherwise copy the on_page_recod to preallocated_memory and return the copy
	memory_move(preallocated_memory, on_page_record, get_tuple_size(attd_p->record_def, on_page_record));
	return preallocated_memory;
}

int set_record_entry_at_child_index_in_array_table_leaf_page(persistent_page* ppage, uint32_t child_index, const void* record, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// child_index out of range
	if(child_index >= attd_p->leaf_entries_per_page)
		return 0;

	if(record == NULL)
	{
		// we need to set NULL at child_index
		// we do not need to use the update_resiliently, because record_def is fixed width tuple
		// if the child_index is greater than or equal to the tuple_count on the page, then this function fails and we do not need to take care of it, since the tuple still get read as NULL by the getter function above
		int updated_to_null = update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def), child_index, NULL, abort_error);
		if(*abort_error)
			return 0;

		// if the entry was update to null and the child_index happens to be at the last tuple then we discard trailing tomb stones
		if(updated_to_null && child_index == (get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def)) - 1))
		{
			discard_trailing_tomb_stones_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def), abort_error);
			if(*abort_error)
				return 0;
		}
	}
	else
	{
		// keep on appending NULLs, until child_index is not in bounds of the tuple_count on the page
		while(child_index >= get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def)))
		{
			append_tuple_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def), NULL, abort_error);
			if(*abort_error)
				return 0;
		}

		// now since the child_index is within bounds of tuple_count, we can directly update	
		update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def), child_index, record, abort_error);
		if(*abort_error)
			return 0;
	}

	return 1;
}

uint64_t get_child_page_id_at_child_index_in_array_table_index_page(const persistent_page* ppage, uint32_t child_index, const array_table_tuple_defs* attd_p)
{
	// child_index out of range
	if(child_index >= attd_p->index_entries_per_page)
		return attd_p->pas_p->NULL_PAGE_ID;

	const void* child_tuple = get_nth_tuple_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), child_index);

	// if the child_tuple is NULL, then it is NULL_PAGE_ID
	if(child_tuple == NULL)
		return attd_p->pas_p->NULL_PAGE_ID;

	// the tuple has only 1 non-NULLable UINT value, hence we can directly access it
	return get_value_from_element_from_tuple(attd_p->index_def, 0, child_tuple).uint_value;
}

int set_child_page_id_at_child_index_in_array_table_index_page(persistent_page* ppage, uint32_t child_index, uint64_t child_page_id, const array_table_tuple_defs* attd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// child_index out of range
	if(child_index >= attd_p->index_entries_per_page)
		return 0;

	// the child_page_id at child_index is already the desired value, then return 1 (success)
	// this allows us to not insert redundant write-ahead_log records
	if(child_page_id == get_child_page_id_at_child_index_in_array_table_index_page(ppage, child_index, attd_p))
		return 1;

	if(child_page_id == attd_p->pas_p->NULL_PAGE_ID)
	{
		// we need to set NULL at child_index
		// we do not need to use the update_resiliently, because index_def is fixed width tuple with only 1 element
		// if the child_index is greater than or equal to the tuple_count on the page, then this function fails and we do not need to take care of it, since the tuple still get read as NULL by the getter function above
		int updated_to_null = update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), child_index, NULL, abort_error);
		if(*abort_error)
			return 0;

		// if the entry was update to null and the child_index happens to be at the last tuple then we discard trailing tomb stones
		if(updated_to_null && child_index == (get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def)) - 1))
		{
			discard_trailing_tomb_stones_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), abort_error);
			if(*abort_error)
				return 0;
		}
	}
	else
	{
		// keep on appending NULLs, until child_index is not in bounds of the tuple_count on the page
		while(child_index >= get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def)))
		{
			append_tuple_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), NULL, abort_error);
			if(*abort_error)
				return 0;
		}

		// now since the child_index is within bounds of tuple_count, we can directly update

		// construct a tuple in temporary memory and make it point to child_page_id
		void* new_child_tuple = malloc(get_minimum_tuple_size(attd_p->index_def)); // minimum size of fixed width tuple is same as its size
		if(new_child_tuple == NULL)  // memory allocation failed
			exit(-1);
		init_tuple(attd_p->index_def, new_child_tuple);
		set_element_in_tuple(attd_p->index_def, 0, new_child_tuple, &((user_value){.uint_value = child_page_id}));

		// perform update
		update_tuple_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), child_index, new_child_tuple, abort_error);
		free(new_child_tuple);
		if(*abort_error)
			return 0;
	}

	return 1;
}

int has_all_NULL_PAGE_ID_in_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	// if tomb_stone_count == tuple_count, then this means all child_page_id's are NULL_PAGE_ID
	if(is_array_table_leaf_page(ppage, attd_p))
		return get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def)) == get_tomb_stone_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def));
	else
		return get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def)) == get_tomb_stone_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def));
}

uint32_t get_non_NULL_PAGE_ID_count_in_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	// this is simply equal to tuple_count - tomb_stone_count
	if(is_array_table_leaf_page(ppage, attd_p))
		return get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def)) - get_tomb_stone_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->record_def->size_def));
	else
		return get_tuple_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def)) - get_tomb_stone_count_on_persistent_page(ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def));
}

bucket_range get_bucket_range_for_array_table_page(const persistent_page* ppage, const array_table_tuple_defs* attd_p)
{
	array_table_page_header hdr = get_array_table_page_header(ppage, attd_p);

	bucket_range result = {.first_bucket_id = hdr.first_bucket_id};

	uint64_t bucket_range_size;
	if(0 == get_leaf_entries_refrenceable_by_entry_at_given_level_using_array_table_tuple_definitions(attd_p, hdr.level + 1, &bucket_range_size))
		goto EXIT_OVERFLOW;

	if(will_unsigned_sum_overflow(uint64_t, result.first_bucket_id, (bucket_range_size-1)))
		goto EXIT_OVERFLOW;

	result.last_bucket_id = result.first_bucket_id + (bucket_range_size-1);

	// effectively equal to [first_bucket_id, first_bucket_id + get_leaf_entries_refrenceable(level + 1) - 1]
	return result;

	EXIT_OVERFLOW:;
	result.last_bucket_id = UINT64_MAX;
	return result;
}

bucket_range get_delegated_bucket_range_for_child_index_on_array_table_page(const persistent_page* ppage, uint32_t child_index, const array_table_tuple_defs* attd_p)
{
	// child_index must be within bounds of entries_per_page
	if( ( is_array_table_leaf_page(ppage, attd_p) && child_index >= attd_p->leaf_entries_per_page) ||
		(!is_array_table_leaf_page(ppage, attd_p) && child_index >= attd_p->index_entries_per_page))
		goto EXIT_OUT_OF_BOUNDS_CHILD;

	array_table_page_header hdr = get_array_table_page_header(ppage, attd_p);

	bucket_range result;

	uint64_t child_bucket_range_size;
	if(0 == get_leaf_entries_refrenceable_by_entry_at_given_level_using_array_table_tuple_definitions(attd_p, hdr.level, &child_bucket_range_size))
	{
		if(child_index != 0)
			goto EXIT_OUT_OF_BOUNDS_CHILD;
		else
		{
			// if the child_index is 0, then the first_bucket_id will not overflow but the last_bucket_id will
			result = (bucket_range){.first_bucket_id = hdr.first_bucket_id};
			goto EXIT_OVERFLOW;
		}
	}

	if(will_unsigned_mul_overflow(uint64_t, child_bucket_range_size, child_index))
		goto EXIT_OUT_OF_BOUNDS_CHILD;
	if(will_unsigned_sum_overflow(uint64_t, hdr.first_bucket_id, child_bucket_range_size * child_index))
		goto EXIT_OUT_OF_BOUNDS_CHILD;

	result = (bucket_range){.first_bucket_id = (hdr.first_bucket_id + child_bucket_range_size * child_index)};

	if(will_unsigned_sum_overflow(uint64_t, result.first_bucket_id, (child_bucket_range_size-1)))
		goto EXIT_OVERFLOW;

	result.last_bucket_id = result.first_bucket_id + (child_bucket_range_size-1);

	// effectively equal to [first_bucket_id + (child_index * get_leaf_entries_refrenceable(level)), first_bucket_id + (child_index * get_leaf_entries_refrenceable(level)) + get_leaf_entries_refrenceable(level) - 1]
	return result;

	EXIT_OVERFLOW:;
	result.last_bucket_id = UINT64_MAX;
	return result;

	// this is what happens when even the first_bucket_id of the child's delegated range is out of 64 bit number
	EXIT_OUT_OF_BOUNDS_CHILD:
	return (bucket_range){.first_bucket_id = UINT64_MAX, .last_bucket_id = 0};
}

uint32_t get_child_index_for_bucket_id_on_array_table_page(const persistent_page* ppage, uint64_t bucket_id, const array_table_tuple_defs* attd_p)
{
	bucket_range bucket_range_for_page = get_bucket_range_for_array_table_page(ppage, attd_p);

	if(!is_bucket_contained_bucket_range(&bucket_range_for_page, bucket_id))
		return NO_TUPLE_FOUND;

	array_table_page_header hdr = get_array_table_page_header(ppage, attd_p);

	uint64_t child_bucket_range_size;
	if(0 == get_leaf_entries_refrenceable_by_entry_at_given_level_using_array_table_tuple_definitions(attd_p, hdr.level, &child_bucket_range_size)) // if child_bucket_range_size is too big then, you can only access the first child
		return 0;

	// effectively equal to (bucket_id - first_bucket_id) / get_leaf_entries_refrenceable(level)
	return (bucket_id - bucket_range_for_page.first_bucket_id) / child_bucket_range_size;
}

int level_up_array_table_page(persistent_page* ppage, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// grab the header of the page
	array_table_page_header hdr = get_array_table_page_header(ppage, attd_p);

	// cache the old header value
	const array_table_page_header old_hdr = hdr;
	uint64_t only_child_page_id = attd_p->pas_p->NULL_PAGE_ID;

	// if the ppage has atleast 1 non-NULL_PAGE_ID, then its contents have to preserved in its only child
	if(!has_all_NULL_PAGE_ID_in_array_table_page(ppage, attd_p))
	{
		// we do not need to create a new child page, if the ppage has only 1 child and ppage is not a leaf
		if(get_non_NULL_PAGE_ID_count_in_array_table_page(ppage, attd_p) == 1 && !is_array_table_leaf_page(ppage, attd_p))
		{
			// find the only child (only_child_page_id) of this ppage
			for(uint32_t i = 0; i < attd_p->index_entries_per_page; i++) // here we are using index_entries_per_page, as we already checked that it is not a leaf page
			{
				if(get_child_page_id_at_child_index_in_array_table_index_page(ppage, i, attd_p) != attd_p->pas_p->NULL_PAGE_ID)
				{
					only_child_page_id = get_child_page_id_at_child_index_in_array_table_index_page(ppage, i, attd_p);
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
			if(is_array_table_leaf_page(ppage, attd_p)) // at this point ppage can be a leaf page or an index page, hence take care of both the cases
			{
				clone_persistent_page(pmm_p, transaction_id, &only_child_page, attd_p->pas_p->page_size, &(attd_p->record_def->size_def), ppage, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, NONE_OPTION, abort_error);
					return 0;
				}
			}
			else
			{
				clone_persistent_page(pmm_p, transaction_id, &only_child_page, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), ppage, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, NONE_OPTION, abort_error);
					return 0;
				}
			}

			// now we are done with only_child_page, release lock on it
			only_child_page_id = only_child_page.page_id;
			release_lock_on_persistent_page(pam_p, transaction_id, &only_child_page, NONE_OPTION, abort_error);
			if(*abort_error)
				return 0;
		}
	}

	// increment its level and update first_bucket_id
	hdr.level = old_hdr.level + 1;
	hdr.first_bucket_id = get_first_bucket_id_for_level_containing_bucket_id_for_array_table_page(old_hdr.level + 1, old_hdr.first_bucket_id, attd_p);

	// write new header hdr onto the ppage
	set_array_table_page_header(ppage, &hdr, attd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	// from this point on, level of ppage is > 0 (because we incremented it), hence we can operate on it with index_def and index access functions

	// discard all tuples from the ppage, this page has a level > 0, hence it must always be an index page
	discard_all_tuples_on_persistent_page(pmm_p, transaction_id, ppage, attd_p->pas_p->page_size, &(attd_p->index_def->size_def), abort_error);
	if(*abort_error)
		return 0;

	// if we needed to copy contents of the ppage onto its child, then make an entry for it on this promoted ppage
	if(only_child_page_id != attd_p->pas_p->NULL_PAGE_ID)
	{
		// the entry goes for only_child_page.first_bucket_id -> only_child_page_id
		uint32_t child_index_for_only_child_page = get_child_index_for_bucket_id_on_array_table_page(ppage, old_hdr.first_bucket_id, attd_p);
		set_child_page_id_at_child_index_in_array_table_index_page(ppage, child_index_for_only_child_page, only_child_page_id, attd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	return 1;
}

int level_down_page_table_page(persistent_page* ppage, const array_table_tuple_defs* attd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// if the page is leaf OR if the count of non-NULL_PAGE_ID entry is not 1, then it can can not be levelled down
	if(is_page_table_leaf_page(ppage, attd_p) || get_non_NULL_PAGE_ID_count_in_page_table_page(ppage, attd_p) != 1)
		return 0;

	// find the only child (only_child_page_id) of this ppage
	uint64_t only_child_page_id = attd_p->pas_p->NULL_PAGE_ID;
	for(uint32_t i = 0; i < attd_p->entries_per_page; i++)
	{
		if(get_child_page_id_at_child_index_in_page_table_page(ppage, i, attd_p) != attd_p->pas_p->NULL_PAGE_ID)
		{
			only_child_page_id = get_child_page_id_at_child_index_in_page_table_page(ppage, i, attd_p);
			break;
		}
	}

	// get write lock on this only_child_page
	persistent_page only_child_page = acquire_persistent_page_with_lock(pam_p, transaction_id, only_child_page_id, WRITE_LOCK, abort_error);
	if(*abort_error)
		return 0;

	// clone the only_child_page onto the ppage
	clone_persistent_page(pmm_p, transaction_id, ppage,  attd_p->pas_p->page_size, &(attd_p->entry_def->size_def), &only_child_page, abort_error);
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