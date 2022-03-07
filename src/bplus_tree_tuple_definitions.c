#include<bplus_tree_tuple_definitions.h>

#include<stdlib.h>
#include<string.h>

void init_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p, const tuple_def* record_def, uint32_t key_element_count)
{
	bpttd_p->key_def = malloc(size_of_tuple_def(key_element_count));
	memmove(bpttd_p->key_def, record_def, size_of_tuple_def(key_element_count));
	bpttd_p->key_def->element_count = key_element_count;
	finalize_tuple_def(bpttd_p->key_def);

	bpttd_p->index_def = malloc(size_of_tuple_def(key_element_count + 1));
	memmove(bpttd_p->index_def, record_def, size_of_tuple_def(key_element_count));
	bpttd_p->index_def->element_count = key_element_count;
	insert_element_def(bpttd_p->index_def, "to_page_id", UINT, 4);
	finalize_tuple_def(bpttd_p->index_def);

	bpttd_p->record_def = malloc(size_of_tuple_def(record_def->element_count));
	memmove(bpttd_p->record_def, record_def, size_of_tuple_def(record_def->element_count));
	finalize_tuple_def(bpttd_p->record_def);
}

void deinit_bplus_tree_tuple_definitions(bplus_tree_tuple_defs* bpttd_p)
{
	free(bpttd_p->key_def);
	free(bpttd_p->index_def);
	free(bpttd_p->record_def);
}