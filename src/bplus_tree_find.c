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

bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
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

	while(!is_bplus_tree_leaf_page(curr_page, bpttd_p->page_size))
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
				// TODO
				break;
			}
			case LESSER_THAN_EQUALS_KEY :
			{
				// TODO
				break;
			}
			case GREATER_THAN_EQUALS_KEY :
			{
				// TODO
				break;
			}
			case GREATER_THAN_KEY :
			{
				// TODO
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

	if(f_type == MIN_TUPLE)
		curr_tuple_index = 0;
	else if(f_type == MAX_TUPLE)
		curr_tuple_index = LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
	else
	{
		//TODO
		// use several of sorted_packed_page_util functions to figure out which tuple to start from
	}

	bplus_tree_iterator* bpi_p = get_new_bplus_tree_iterator(curr_page, curr_page_id, curr_tuple_index, bpttd_p, dam_p);

	// iterate next or previous in bplus_tree_iterator, based on the f_type
	// this is not required for MIN_TUPLE and MAX_TUPLE
	if(f_type != MIN_TUPLE && f_type != MAX_TUPLE)
	{
		// TODO
	}

	return bpi_p;
}