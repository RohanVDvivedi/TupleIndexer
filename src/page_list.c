#include<page_list.h>

uint32_t get_new_page_list(const data_access_methods* dam_p);

void initialize_cursor(page_cursor* pc_p, uint32_t page_list_page_id, const data_access_methods* dam_p)
{
	pc_p->page_id = page_list_page_id;
	pc_p->page = NULL;
	pc_p->tuple_index = 0;
	pc_p->tuple = NULL;
}

int seek_cursor_to_next_tuple(page_cursor* pc_p, const data_access_methods* dam_p);

int delete_tuple_at_the_cursor(page_cursor* pc_p, const void* tuple_like, const data_access_methods* dam_p);

int insert_tuple_after_the_cursor(page_cursor* pc_p, const void* tuple_like, const data_access_methods* dam_p);

void deinitialize_cursor(page_cursor* pc_p)
{

}

void external_merge_sort_the_page_list(uint32_t page_list_head_page_id, uint16_t key_elements_count, const data_access_methods* dam_p);
