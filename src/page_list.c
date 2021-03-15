#include<page_list.h>

#include<tuple.h>
#include<page_layout.h>

static inline void* acquire_lock(page_cursor* pc_p, uint32_t page_id)
{
	switch(pc_p->lock_type)
	{
		READER_LOCK :
			return pc_p->dam_p->acquire_page_with_reader_lock(pc_p->dam_p->context, &new_page_list_head_page_id);
		WRITER_LOCK :
			return pc_p->dam_p->acquire_page_with_writer_lock(pc_p->dam_p->context, &new_page_list_head_page_id);
	}
}

// call this function only if pc_p->lock_type == WRITER_LOCK
static inline void* acquire_new_page_with_lock(page_cursor* pc_p, uint32_t* page_id_p)
{
	return pc_p->dam_p->get_new_page_with_write_lock(pc_p->dam_p->context, page_id_p);
}

static inline int release_lock(page_cursor* pc_p, void* page)
{
	switch(pc_p->lock_type)
	{
		READER_LOCK :
			return pc_p->dam_p->release_reader_lock_on_page(pc_p->dam_p->context, &page);
		WRITER_LOCK :
			return pc_p->dam_p->release_writer_lock_on_page(pc_p->dam_p->context, &page);
	}
}

static inline int release_lock_and_free_page(page_cursor* pc_p, void* page)
{
	switch(pc_p->lock_type)
	{
		READER_LOCK :
			return pc_p->dam_p->release_reader_lock_and_free_page(pc_p->dam_p->context, &page);
		WRITER_LOCK :
			return pc_p->dam_p->release_reader_lock_and_free_page(pc_p->dam_p->context, &page);
	}
}

uint32_t get_new_page_list(const data_access_methods* dam_p)
{
	uint32_t new_page_list_head_page_id = 0;
	void* new_page_list_head_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_page_list_head_page_id);
	init_page(new_page_list_head_page, dam_p->page_size, 1, 2, NULL);
	dam_p->release_writer_lock_on_page(dam_p->context, new_page_list_head_page);
	return new_page_list_head_page_id;
}

int initialize_cursor(page_cursor* pc_p, page_cursor_lock_type lock_type, page_cursor_traversal_direction traverse_dir, uint32_t page_list_page_id, const tuple_def* tpl_d, const data_access_methods* dam_p)
{
	pc_p->lock_type = lock_type;
	pc_p->traverse_dir = traverse_dir;

	pc_p->tpl_d = tpl_d;
	pc_p->dam_p = dam_p;

	pc_p->page_id = page_list_page_id;
	pc_p->tuple_index = 0;

	pc_p->page = acquire_lock(pc_p, page_list_page_id);
	if(pc_p->page == NULL || get_tuple_count(pc_p->page) == 0)
		pc_p->tuple = NULL;
	else
		pc_p->tuple = seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_index);

	return pc_p->tuple == NULL;
}

int seek_cursor_to_next_page(page_cursor* pc_p)
{
	if(pc_p->page == NULL)
		return 0;

	uint32_t next_page_id = get_references_page_id(pc_p->page, NEXT_PAGE_REFERENCE_INDEX);
	void* old_page = pc_p->page_id;

	// can not jump to a NULL page
	if(next_page_id == NULL_PAGE_REFERENCE)
		return 0;

	pc_p->page_id = new_page_id;
	pc_p->tuple_index = 0;

	pc_p->page = acquire_lock(pc_p, next_page_id);
	release_lock(pc_p, old_page);

	if(pc_p->page == NULL || get_tuple_count(pc_p->page) == 0)
		pc_p->tuple = NULL;
	else
		pc_p->tuple = seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_index);

	return pc_p->tuple == NULL;
}

int seek_cursor_to_next_tuple(page_cursor* pc_p)
{
	if(pc_p->page == NULL)
		return 0;

	if(pc_p->tuple_index == get_tuple_count(pc_p->page))
		return seek_cursor_to_next_page(pc_p);
	else
		pc_p->tuple = seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_index++);

	return pc_p->tuple == NULL;
}

int insert_tuple_after_the_cursor(page_cursor* pc_p, const void* tuple_to_insert)
{

}

int delete_tuple_at_the_cursor(page_cursor* pc_p)
{

}

void deinitialize_cursor(page_cursor* pc_p)
{

}

void external_merge_sort_the_page_list(uint32_t page_list_head_page_id, uint16_t key_elements_count, const data_access_methods* dam_p);
