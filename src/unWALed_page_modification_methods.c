#include<unWALed_page_modification_methods.h>

#include<page_layout.h>

#include<stdlib.h>

static int init_page_unWALed(void* context, persistent_page page, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d);

static int set_page_header_unWALed(void* context, persistent_page page, uint32_t page_size, const void* hdr);

static int append_tuple_on_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple);

static int update_tuple_on_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

static int discard_tuple_on_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index);

static int discard_all_tuples_on_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d);

static int discard_trailing_tomb_stones_on_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t* tomb_stones_discarded);

static int swap_tuples_on_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2);

static int set_element_in_tuple_in_place_on_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, uint32_t element_index, const user_value* value);

static int clone_page_unWALed(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page page_src);

page_modification_methods* get_new_unWALed_page_modification_methods()
{
	page_modification_methods* pmm_p = malloc(sizeof(page_modification_methods));
	if(pmm_p == NULL)
		return NULL;

	pmm_p->init_page = init_page_unWALed;
	pmm_p->set_page_header = set_page_header_unWALed;
	pmm_p->append_tuple_on_page = append_tuple_on_page_unWALed;
	pmm_p->update_tuple_on_page = update_tuple_on_page_unWALed;
	pmm_p->discard_tuple_on_page = discard_tuple_on_page_unWALed;
	pmm_p->discard_all_tuples_on_page = discard_all_tuples_on_page_unWALed;
	pmm_p->discard_trailing_tomb_stones_on_page = discard_trailing_tomb_stones_on_page_unWALed;
	pmm_p->swap_tuples_on_page = swap_tuples_on_page_unWALed;
	pmm_p->set_element_in_tuple_in_place_on_page = set_element_in_tuple_in_place_on_page_unWALed;
	pmm_p->clone_page = clone_page;

	pmm_p->context = NULL;

	return pmm_p;
}

void delete_unWALed_page_modification_methods(page_modification_methods* pmm_p)
{
	free(pmm_p);
}