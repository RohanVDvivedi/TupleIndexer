#include<bplus_tree_leaf_page_util.h>

#include<tuple.h>
#include<page_layout.h>

#include<sorted_packed_page_util.h>
#include<storage_capacity_page_util.h>

#include<stdlib.h>
#include<string.h>

int is_leaf_page(const void* page)
{
	return get_page_type(page) == LEAF_PAGE_TYPE;
}

int init_leaf_page(void* page, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	int inited = init_page(page, page_size, LEAF_PAGE_TYPE, 1, bpttds->record_def);
	if(inited)
		set_next_sibling_leaf_page(page, NULL_PAGE_REF);
	return inited;
}

uint32_t get_next_sibling_leaf_page(const void* page)
{
	return get_reference_page_id(page, NEXT_SIBLING_PAGE_REF);
}

int set_next_sibling_leaf_page(void* page, uint32_t next_page_id)
{
	return set_reference_page_id(page, NEXT_SIBLING_PAGE_REF, next_page_id);
}

uint16_t get_record_count_in_leaf_page(const void* page)
{
	return get_tuple_count(page);
}

const void* get_record_from_leaf_page(const void* page, uint32_t page_size, uint16_t index, const bplus_tree_tuple_defs* bpttds)
{
	return get_nth_tuple(page, page_size, bpttds->record_def, index);
}

static int can_split_leaf_page(void* page_to_be_split, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// can not split if the page is less than half full
	if(is_page_lesser_than_or_equal_to_half_full(page_to_be_split, page_size, bpttds->record_def))
		return 0;

	return 1;
}

void* split_insert_leaf_page(void* page_to_be_split, const void* new_record, void* new_page, uint32_t new_page_id, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// if can not split the page
	if(!can_split_leaf_page(page_to_be_split, page_size, bpttds))
		return NULL;

	// record_count before split
	uint16_t record_count = get_record_count_in_leaf_page(page_to_be_split);

	// initialize temp page
	uint32_t temp_page_size = 2 * page_size;
	void* temp_page = malloc(temp_page_size);

	{
		// init as leaf page
		init_leaf_page(temp_page, temp_page_size, bpttds);

		// insert all records to the temp_page (including the new record)
		for(uint16_t i = 0; i < record_count; i++)
		{
			const void* tuple_to_move = get_nth_tuple(page_to_be_split, page_size, bpttds->record_def, i);
			insert_tuple(temp_page, temp_page_size, bpttds->record_def, tuple_to_move);
		}
		insert_to_sorted_packed_page(temp_page, temp_page_size, bpttds->key_def, bpttds->record_def, new_record, NULL);

		// and reset both the pages, to insert the split tuples

		// init new leaf page
		init_leaf_page(new_page, page_size, bpttds);

		// delete all in page_to_be_split
		delete_all_tuples(page_to_be_split, page_size, bpttds->record_def);

		// fix sibling references of both the pages
		uint32_t next_of_page_to_be_split = get_next_sibling_leaf_page(page_to_be_split);
		set_next_sibling_leaf_page(new_page, next_of_page_to_be_split);
		set_next_sibling_leaf_page(page_to_be_split, new_page_id);
	}

	// this is the new record count, considering that the new tuple is inserted to this one temp page, instead of 2 split pages
	record_count = get_record_count_in_leaf_page(temp_page);

	// actual move operation, to move tuples to either of page_to_be_split or to new_page
	{
		uint16_t tuple_to_insert = 0;

		while(is_page_lesser_than_half_full(page_to_be_split, page_size, bpttds->record_def))
		{
			const void* tuple_to_move = get_nth_tuple(temp_page, temp_page_size, bpttds->record_def, tuple_to_insert);
			int inserted = insert_tuple(page_to_be_split, page_size, bpttds->record_def, tuple_to_move);
			if(inserted)
				tuple_to_insert++;
			else
				break;
		}

		for(; tuple_to_insert < record_count; tuple_to_insert++)
		{
			const void* tuple_to_move = get_nth_tuple(temp_page, temp_page_size, bpttds->record_def, tuple_to_insert);
			insert_tuple(new_page, page_size, bpttds->record_def, tuple_to_move);
		}
	}

	free(temp_page);

	// build the parent_index that we need to return
	{
		// get first record in the new page
		const void* first_new_page_record = get_record_from_leaf_page(new_page, page_size, 0, bpttds);
		uint32_t first_new_page_key_size = get_tuple_size(bpttds->key_def, first_new_page_record);

		// generate the tuple with index_def, that you need to insert in the parent page
		void* parent_insert = malloc(first_new_page_key_size + 4);
		memmove(parent_insert, first_new_page_record, first_new_page_key_size);
		copy_element_to_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &new_page_id);

		return parent_insert;
	}
}

static int can_merge_leaf_pages(void* page, void* sibling_page_to_be_merged, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// atleast one of the two pages must be lesser than half full
	if(is_page_more_than_or_equal_to_half_full(page, page_size, bpttds->record_def) &&
		is_page_more_than_or_equal_to_half_full(sibling_page_to_be_merged, page_size, bpttds->record_def))
		return 0;

	// check if all the tuples in the sibling page can be inserted into the page
	if(get_free_space_in_page(page, page_size, bpttds->record_def) < get_space_occupied_by_all_tuples(sibling_page_to_be_merged, page_size, bpttds->record_def))
		return 0;

	return 1;
}

int merge_leaf_pages(void* page, const void* parent_index_record, void* sibling_page_to_be_merged, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	// check if pages can be merged
	if(!can_merge_leaf_pages(page, sibling_page_to_be_merged, page_size, bpttds))
		return 0;

	// tuples in the sibling page, that we need to copy to the page
	uint16_t tuples_to_be_merged = get_tuple_count(sibling_page_to_be_merged);

	if(tuples_to_be_merged > 0)
	{
		// insert sibling page entriess to page
		insert_all_from_sorted_packed_page(page, sibling_page_to_be_merged, page_size, bpttds->key_def, bpttds->record_def, 0, tuples_to_be_merged - 1);

		// delete all tuples in the sibling page (that is to be free, hence a redundant operation)
		delete_all_in_sorted_packed_page(sibling_page_to_be_merged, page_size, bpttds->record_def, 0, tuples_to_be_merged - 1);
	}

	// fix sibling references
	uint32_t new_next_page_id = get_next_sibling_leaf_page(sibling_page_to_be_merged);
	set_next_sibling_leaf_page(page, new_next_page_id);

	// again a redundant operation
	set_next_sibling_leaf_page(sibling_page_to_be_merged, NULL_PAGE_REF);

	return 1;
}
