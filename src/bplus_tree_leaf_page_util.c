#include<bplus_tree_leaf_page_util.h>

#include<tuple.h>
#include<page_layout.h>

#include<sorted_packed_page_util.h>

#include<stdlib.h>
#include<string.h>

int is_leaf_page(const void* page)
{
	return get_page_type(page) == LEAF_PAGE_TYPE;
}

int init_leaf_page(void* page, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	return init_page(page, page_size, LEAF_PAGE_TYPE, 2, bpttds->record_def);
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

const void* split_leaf_page(void* page_to_be_split, void* new_page, uint32_t new_page_id, uint32_t page_size, const bplus_tree_tuple_defs* bpttds)
{
	uint16_t record_count = get_record_count_in_leaf_page(page_to_be_split);

	// can not split a page with lesser than 2 records
	if(record_count < 2)
		return NULL;

	// after the split
	uint16_t new_record_count = record_count / 2;

	uint16_t records_to_move_start = new_record_count;
	uint16_t records_to_move_last = record_count;

	// init a new leaf page
	init_leaf_page(new_page, page_size, bpttds);

	// insert records to new page
	insert_all_from_sorted_packed_page(new_page, page_to_be_split, page_size, bpttds->key_def, bpttds->record_def, records_to_move_start, records_to_move_last);

	// delete the copied records in the page that is to be split
	delete_all_in_sorted_packed_page(page_to_be_split, page_size, bpttds->record_def, records_to_move_start, records_to_move_last);

	uint32_t next_of_page_to_be_split = get_next_sibling_leaf_page(page_to_be_split);
	set_next_sibling_leaf_page(new_page, next_of_page_to_be_split);
	set_next_sibling_leaf_page(page_to_be_split, new_page_id);

	// get first record in the new page
	const void* first_new_page_record = get_record_from_leaf_page(new_page, page_size, 0, bpttds);
	uint32_t first_new_page_record_size = get_tuple_size(bpttds->key_def, first_new_page_record);

	// generate the tuple with index_def, that you need to insert in the parent page
	uint32_t parent_insert_tuple_size = first_new_page_record_size + 4;
	void* parent_insert = malloc(parent_insert_tuple_size);
	memmove(parent_insert, first_new_page_record, first_new_page_record_size);
	copy_element_to_tuple(bpttds->index_def, bpttds->index_def->element_count - 1, parent_insert, &new_page_id);

	return parent_insert;
}