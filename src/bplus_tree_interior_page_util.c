#include<bplus_tree_interior_page_util.h>

#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

uint32_t find_child_index_for_key(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	// find preceding equals in the interior pages, by comparing against all index entries
	uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);

	return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
}

uint64_t find_child_page_id_by_child_index(const void* page, uint32_t index, const bplus_tree_tuple_defs* bpttd_p)
{
	// if the index is -1, return the page_id stored in the header
	if(index == -1)
		return get_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p);

	// result child_page_id
	uint64_t child_page_id = 0;

	// tuple of the interior page that is at index
	void* index_tuple = get_nth_tuple(page, bpttd_p->page_size, bpttd_p->index_def, index);

	// populate child_id using the appropriate element at the end of the tuple
	switch(bpttd_p->page_id_width)
	{
		case 1:
		{
			uint8_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 2:
		{
			uint16_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 4:
		{
			uint32_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 8:
		{
			uint64_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
	}

	return child_page_id;
}