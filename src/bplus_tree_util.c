#include<bplus_tree_util.h>

#include<stdlib.h>
#include<string.h>

bplus_tree_tuple_defs* get_bplus_tree_tuple_defs_from_record_def(tuple_def* record_def, uint16_t key_element_count)
{
	if(key_element_count == 0 || is_empty_tuple_def(record_def) || key_element_count > record_def->element_count)
		return NULL;

	bplus_tree_tuple_defs* bpttds = malloc(sizeof(tuple_def*) * 3);

	bpttds->key_def = malloc(size_of_tuple_def(key_element_count));
	memcpy(bpttds->key_def, record_def, size_of_tuple_def(key_element_count));
	bpttds->key_def->element_count = key_element_count;

	bpttds->index_def = malloc(size_of_tuple_def(key_element_count + 1));
	memcpy(bpttds->index_def, record_def, size_of_tuple_def(key_element_count));
	bpttds->index_def->element_count = key_element_count;
	insert_element_def(bpttds->index_def, UINT, 4);

	bpttds->record_def = malloc(size_of_tuple_def(record_def->element_count));
	memcpy(bpttds->record_def, record_def, size_of_tuple_def(record_def->element_count));
	
	return bpttds;
}

void del_bplus_tree_tuple_defs(bplus_tree_tuple_defs* bpttds)
{
	free(bpttds->key_def);
	free(bpttds->index_def);
	free(bpttds->record_def);
	free(bpttds);
}