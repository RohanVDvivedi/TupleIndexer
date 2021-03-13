#include<page_list.h>

static int is_head_page(page_list* pl_p, uint32_t page_id)
{
	return (pl_p->head_page_id != NULL_PAGE_REFERENCE) && (pl_p->head_page_id == page_id);
}

int initialize_page_list(page_list* pl_p, uint32_t head_page_id, tuple_def* tpl_d);

int insert_at_end_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int deinitialize_page_list(page_list* pl_p);

int initialize_cursor(page_cursor* pc_p, page_list* pl_p, const data_access_methods* dam_p);

int delete_tuple_at_the_cursor(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int seek_to_next_tuple_in_page_list(page_cursor* pc_p, const data_access_methods* dam_p);

int deinitialize_cursor(page_cursor* pc_p);

void external_merge_sort_the_page_list(page_list* pl_p, uint16_t key_elements_count, const data_access_methods* dam_p);
