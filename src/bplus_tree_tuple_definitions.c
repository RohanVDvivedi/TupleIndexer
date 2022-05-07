#include<bplus_tree_tuple_definitions.h>

#include<stdlib.h>
#include<string.h>

int init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, const tuple_def* record_def, const uint32_t* key_element_ids, uint32_t key_element_count, uint32_t page_size, int page_id_width)
{
	// basic parameter check
	if(key_element_count == 0 || key_element_ids == NULL || record_def == NULL || record_def->element_count == 0)
		return 0;

	// bytes required to store page id
	if(!(page_id_width == 1 || page_id_width == 2 || page_id_width == 4 || page_id_width == 8))
		return 0;

	// initialize struct attributes
	bpttd_p->page_id_width = page_id_width;
	bpttd_p->page_size = page_size;
	bpttd_p->key_element_count = key_element_count;
	bpttd_p->key_element_ids = key_element_ids;
	bpttd_p->record_def = record_def;

	// allocate memory for index def and initialize it
	bpttd_p->index_def = malloc(size_of_tuple_def(key_element_count + 1));
	init_tuple_def(bpttd_p->index_def, "temp_index_def");

	// result of inserting element_definitions to index def
	int res = 1;

	// initialize element_defs of the index_def
	for(uint32_t i = 0; i < key_element_count && res == 1; i++)
		res = insert_copy_of_element_def(bpttd_p->index_def, NULL, bpttd_p->record_def, bpttd_p->key_element_ids[i]);
	
	// the unsigned int page id that the index entry will point to
	switch(page_id_width)
	{
		case 1 :
		{
			res = res && insert_element_def(bpttd_p->index_def, "child_page_id", UINT, 1);
			break;
		}
		case 2 :
		{
			res = res && insert_element_def(bpttd_p->index_def, "child_page_id", UINT, 2);
			break;
		}
		case 4 :
		{
			res = res && insert_element_def(bpttd_p->index_def, "child_page_id", UINT, 4);
			break;
		}
		case 8 :
		{
			res = res && insert_element_def(bpttd_p->index_def, "child_page_id", UINT, 8);
			break;
		}
	}

	// if any of the index element_definitions could not be inserted
	if(!res)
	{
		deinit_bplus_tree_tuple_definitions(bpttd_p);
		return 0;
	}

	// end step
	finalize_tuple_def(bpttd_p->index_def, page_size);

	return 1;
}

void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p)
{
	free(bpttd_p->index_def);

	bpttd_p->key_element_count = 0;
	bpttd_p->key_element_ids = NULL;
	bpttd_p->record_def = NULL;
	bpttd_p->index_def = NULL;
}