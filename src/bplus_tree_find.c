#include<bplus_tree.h>

#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

#include<stdint.h>

// order of the enum values in find_type must remain the same
typedef enum find_type find_type;
enum find_type
{
	MIN_TUPLE,
	LESSER_THAN_KEY,
	LESSER_THAN_EQUALS_KEY,
	GREATER_THAN_EQUALS_KEY,
	GREATER_THAN_KEY,
	MAX_TUPLE,
};

bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// if the user wants to consider all the key elements then
	// set key_element_count_concerned to bpttd_p->key_element_count
	if(key_element_count_concerned == KEY_ELEMENT_COUNT)
		key_element_count_concerned = bpttd_p->key_element_count;

	find_type f_type;

	if(key == NULL && (find_pos == GREATER_THAN || find_pos == GREATER_THAN_EQUALS))
		f_type = MIN_TUPLE;
	else if(key == NULL && (find_pos == LESSER_THAN || find_pos == LESSER_THAN_EQUALS))
		f_type = MAX_TUPLE;
	else
		f_type = LESSER_THAN_KEY + (find_pos - LESSER_THAN);

	// get lock on the root page of the bplus_tree
	uint64_t curr_page_id = root_page_id;
	void* curr_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root_page_id);

	while(!is_bplus_tree_leaf_page(curr_page, bpttd_p))
	{
		uint32_t child_index = 0;

		// get lock on the next page based on the f_type
		switch(f_type)
		{
			case MIN_TUPLE :
			{
				child_index = -1;
				break;
			}
			case LESSER_THAN_KEY :
			{
				child_index = find_child_index_for_key(curr_page, key, key_element_count_concerned, TOWARDS_FIRST_WITH_KEY, bpttd_p);
				break;
			}
			case LESSER_THAN_EQUALS_KEY :
			{
				child_index = find_child_index_for_key(curr_page, key, key_element_count_concerned, TOWARDS_LAST_WITH_KEY, bpttd_p);
				break;
			}
			case GREATER_THAN_EQUALS_KEY :
			{
				child_index = find_child_index_for_key(curr_page, key, key_element_count_concerned, TOWARDS_FIRST_WITH_KEY, bpttd_p);
				break;
			}
			case GREATER_THAN_KEY :
			{
				child_index = find_child_index_for_key(curr_page, key, key_element_count_concerned, TOWARDS_LAST_WITH_KEY, bpttd_p);
				break;
			}
			case MAX_TUPLE :
			{
				child_index = get_tuple_count(curr_page, bpttd_p->page_size, bpttd_p->index_def) - 1;
				break;
			}
		}

		uint64_t next_page_id = find_child_page_id_by_child_index(curr_page, child_index, bpttd_p);
		void* next_page = dam_p->acquire_page_with_reader_lock(dam_p->context, next_page_id);

		// release lock on the curr_page and 
		// make the next_page as the curr_page
		dam_p->release_reader_lock_on_page(dam_p->context, curr_page);
		curr_page = next_page;
		curr_page_id = next_page_id;
	}

	// find the curr_tuple_index for the iterator to start with
	uint32_t curr_tuple_index = 0;

	switch(f_type)
	{
		case MIN_TUPLE :
		{
			curr_tuple_index = 0;
			break;
		}
		case LESSER_THAN_KEY :
		{
			curr_tuple_index = find_preceding_in_sorted_packed_page(
									curr_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			curr_tuple_index = (curr_tuple_index != NO_TUPLE_FOUND) ? curr_tuple_index : 0;
			break;
		}
		case LESSER_THAN_EQUALS_KEY :
		{
			curr_tuple_index = find_preceding_equals_in_sorted_packed_page(
									curr_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			curr_tuple_index = (curr_tuple_index != NO_TUPLE_FOUND) ? curr_tuple_index : 0;
			break;
		}
		case GREATER_THAN_EQUALS_KEY :
		{
			curr_tuple_index = find_succeeding_equals_in_sorted_packed_page(
									curr_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			curr_tuple_index = (curr_tuple_index != NO_TUPLE_FOUND) ? curr_tuple_index : LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
		case GREATER_THAN_KEY :
		{
			curr_tuple_index = find_succeeding_in_sorted_packed_page(
									curr_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			curr_tuple_index = (curr_tuple_index != NO_TUPLE_FOUND) ? curr_tuple_index : LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
		case MAX_TUPLE :
		{
			curr_tuple_index = LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
	}

	bplus_tree_iterator* bpi_p = get_new_bplus_tree_iterator(curr_page, curr_page_id, curr_tuple_index, bpttd_p, dam_p);

	// iterate next or previous in bplus_tree_iterator, based on the f_type
	// this is not required for MIN_TUPLE and MAX_TUPLE
	switch(f_type)
	{
		case LESSER_THAN_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, key_element_count_concerned) >= 0)
			{
				prev_bplus_tree_iterator(bpi_p);
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case LESSER_THAN_EQUALS_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, key_element_count_concerned) > 0)
			{
				prev_bplus_tree_iterator(bpi_p);
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN_EQUALS_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, key_element_count_concerned) < 0)
			{
				next_bplus_tree_iterator(bpi_p);
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, key_element_count_concerned) <= 0)
			{
				next_bplus_tree_iterator(bpi_p);
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		default :
		case MIN_TUPLE :
		case MAX_TUPLE :
			break;
	}

	return bpi_p;
}