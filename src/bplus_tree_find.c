#include<bplus_tree.h>

bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	//void* root_page = dam_p->acquire_page_with_writer_lock(dam_p->context, root_page_id);

	return NULL;
}