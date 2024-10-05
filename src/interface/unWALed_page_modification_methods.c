#include<unWALed_page_modification_methods.h>

#include<page_layout.h>

#include<stdlib.h>

static int init_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	return init_page(ppage.page, page_size, page_header_size, tpl_sz_d);
}

static void set_page_header_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const void* hdr, int* abort_error)
{
	void* pg_hdr = get_page_header(ppage.page, page_size);
	uint32_t pg_hdr_size = get_page_header_size(ppage.page, page_size);
	memory_move(pg_hdr, hdr, pg_hdr_size);
}

static int append_tuple_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error)
{
	return append_tuple_on_page(ppage.page, page_size, tpl_sz_d, external_tuple);
}

static int insert_tuple_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	return insert_tuple_on_page(ppage.page, page_size, tpl_sz_d, index, external_tuple);
}

static int update_tuple_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	return update_tuple_on_page(ppage.page, page_size, tpl_sz_d, index, external_tuple);
}

static int discard_tuple_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, int* abort_error)
{
	return discard_tuple_on_page(ppage.page, page_size, tpl_sz_d, index);
}

static void discard_all_tuples_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	discard_all_tuples_on_page(ppage.page, page_size, tpl_sz_d);
}

static uint32_t discard_trailing_tomb_stones_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	return discard_trailing_tomb_stones_on_page(ppage.page, page_size, tpl_sz_d);
}

static int swap_tuples_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2, int* abort_error)
{
	return swap_tuples_on_page(ppage.page, page_size, tpl_sz_d, i1, i2);
}

static int set_element_in_tuple_in_place_on_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, positional_accessor element_index, const user_value* value, int* abort_error)
{
	return set_element_in_tuple_in_place_on_page(ppage.page, page_size, tpl_d, tuple_index, element_index, value);
}

static void clone_page_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page ppage_src, int* abort_error)
{
	clone_page(ppage.page, page_size, tpl_sz_d, ppage_src.page);
}

static int run_page_compaction_unWALed(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	int memory_allocation_error = 0; // error to be returned to user
	int res = run_page_compaction(ppage.page, page_size, tpl_sz_d, &memory_allocation_error);

	// just like how we do here, in case of an allocation error in pmm, exit()
	// as you see here, a failure to malloc causes us to exit() instead of an abort_error !!
	if(memory_allocation_error)
		exit(-1);

	return res;
}

page_modification_methods* get_new_unWALed_page_modification_methods()
{
	page_modification_methods* pmm_p = malloc(sizeof(page_modification_methods));
	if(pmm_p == NULL)
		return NULL;

	pmm_p->init_page = init_page_unWALed;
	pmm_p->set_page_header = set_page_header_unWALed;
	pmm_p->append_tuple_on_page = append_tuple_on_page_unWALed;
	pmm_p->insert_tuple_on_page = insert_tuple_on_page_unWALed;
	pmm_p->update_tuple_on_page = update_tuple_on_page_unWALed;
	pmm_p->discard_tuple_on_page = discard_tuple_on_page_unWALed;
	pmm_p->discard_all_tuples_on_page = discard_all_tuples_on_page_unWALed;
	pmm_p->discard_trailing_tomb_stones_on_page = discard_trailing_tomb_stones_on_page_unWALed;
	pmm_p->swap_tuples_on_page = swap_tuples_on_page_unWALed;
	pmm_p->set_element_in_tuple_in_place_on_page = set_element_in_tuple_in_place_on_page_unWALed;
	pmm_p->run_page_compaction = run_page_compaction_unWALed;
	pmm_p->clone_page = clone_page_unWALed;

	pmm_p->context = NULL;

	return pmm_p;
}

void delete_unWALed_page_modification_methods(page_modification_methods* pmm_p)
{
	free(pmm_p);
}