#include<sorter.h>

sorter_handle get_new_sorter(const sorter_tuple_defs* std_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

// if there exists a partial unsorted run, then it is sorter and put at the end of the sorted runs
static void consume_partial_unsorted_run_from_sorter(sorter_handle* sh_p, const void* transaction_id, int* abort_error)
{
	// TODO
}

int insert_in_sorter(sorter_handle* sh_p, const void* record, const void* transaction_id, int* abort_error)
{
	// TODO
}

int external_sort_merge_sorter(sorter_handle* sh_p, uint64_t N_way, const void* transaction_id, int* abort_error)
{
	// TODO
}

int destroy_sorter(sorter_handle* sh_p, uint64_t* sorted_data, const void* transaction_id, int* abort_error)
{
	// TODO
}