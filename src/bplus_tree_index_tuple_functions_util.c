#include<bplus_tree_index_tuple_functions_util.h>

#include<tuple.h>

uint64_t get_child_page_id_from_index_tuple(const void* index_tuple, const bplus_tree_tuple_defs* bpttd_p)
{
	uint64_t child_page_id;

	switch(bpttd_p->page_id_width)
	{
		case 1 :
		{
			uint8_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 2 :
		{
			uint16_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 3 :
		case 4 :
		{
			uint32_t cpid;
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid);
			child_page_id = cpid;
			break;
		}
		case 5 :
		case 6 :
		case 7 :
		case 8 :
		default :	// default will and should never occur
		{
			copy_element_from_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &child_page_id);
			break;
		}
	}

	return child_page_id;
}

void set_child_page_id_in_index_tuple(void* index_tuple, uint64_t child_page_id, const bplus_tree_tuple_defs* bpttd_p)
{
	switch(bpttd_p->page_id_width)
	{
		case 1 :
		{
			uint8_t cpid = child_page_id;
			set_element_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid, 1);
			break;
		}
		case 2 :
		{
			uint16_t cpid = child_page_id;
			set_element_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid, 2);
			break;
		}
		case 3 :
		case 4 :
		{
			uint32_t cpid = child_page_id;
			set_element_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &cpid, 4);
			break;
		}
		case 5 :
		case 6 :
		case 7 :
		case 8 :
		default :	// default will and should never occur
		{
			set_element_in_tuple(bpttd_p->index_def, bpttd_p->index_def->element_count - 1, index_tuple, &child_page_id, 8);
			break;
		}
	}
}