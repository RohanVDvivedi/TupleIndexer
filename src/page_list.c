#include<page_list.h>

#include<tuple.h>
#include<page_layout.h>

static inline void* acquire_lock(page_cursor* pc_p, uint32_t new_page_id)
{
	switch(pc_p->lock_type)
	{
		case READER_LOCK :
			return pc_p->dam_p->acquire_page_with_reader_lock(pc_p->dam_p->context, new_page_id);
		case WRITER_LOCK :
			return pc_p->dam_p->acquire_page_with_writer_lock(pc_p->dam_p->context, new_page_id);
	}
	return NULL;
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
		case READER_LOCK :
			return pc_p->dam_p->release_reader_lock_on_page(pc_p->dam_p->context, &page);
		case WRITER_LOCK :
			return pc_p->dam_p->release_writer_lock_on_page(pc_p->dam_p->context, &page);
	}
	return 0;
}

static inline int release_lock_and_free_page(page_cursor* pc_p, void* page)
{
	switch(pc_p->lock_type)
	{
		case READER_LOCK :
			return pc_p->dam_p->release_reader_lock_and_free_page(pc_p->dam_p->context, &page);
		case WRITER_LOCK :
			return pc_p->dam_p->release_reader_lock_and_free_page(pc_p->dam_p->context, &page);
	}
	return 0;
}

uint32_t create_new_page_list(const data_access_methods* dam_p)
{
	uint32_t new_page_list_head_page_id = 0;
	void* new_page_list_head_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_page_list_head_page_id);
	
	// initialize the first page and set both of its references, next and prev to nulls
	init_page(new_page_list_head_page, dam_p->page_size, 1, 2, NULL);
	set_reference_page_id(new_page_list_head_page, NEXT_PAGE_REFERENCE_INDEX, NULL_PAGE_REFERENCE);
	set_reference_page_id(new_page_list_head_page, PREV_PAGE_REFERENCE_INDEX, NULL_PAGE_REFERENCE);
	
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
	if(get_tuple_count(pc_p->page) == 0)
		pc_p->tuple = NULL;
	else
		pc_p->tuple = seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_index);

	return pc_p->tuple != NULL;
}

int seek_cursor_to_current_page_first_tuple(page_cursor* pc_p)
{
	pc_p->tuple_index = 0;
	if(get_tuple_count(pc_p->page) == 0)
		pc_p->tuple = NULL;
	else
		pc_p->tuple = seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_index);

	return pc_p->tuple != NULL;
}

int seek_cursor_to_next_page_first_tuple(page_cursor* pc_p)
{
	uint32_t next_page_id = get_reference_page_id(pc_p->page, NEXT_PAGE_REFERENCE_INDEX);
	void* old_page = pc_p->page;

	// can not jump to a NULL page
	if(next_page_id == NULL_PAGE_REFERENCE)
		return 0;

	pc_p->page_id = next_page_id;
	pc_p->tuple_index = 0;

	pc_p->page = acquire_lock(pc_p, next_page_id);
	release_lock(pc_p, old_page);

	pc_p->tuple = seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_index);

	return pc_p->tuple != NULL;
}

int seek_cursor_to_next_tuple(page_cursor* pc_p)
{
	pc_p->tuple_index++;

	if(pc_p->tuple_index == get_tuple_count(pc_p->page))
		return seek_cursor_to_next_page_first_tuple(pc_p);
	else
		pc_p->tuple = seek_to_nth_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->tuple_index);

	return pc_p->tuple != NULL;
}

int split_page_at_cursor(page_cursor* pc_p);

int insert_tuple_at_cursor(page_cursor* pc_p, int after_cursor, const void* tuple_to_insert)
{
	if(pc_p->lock_type != WRITER_LOCK)
		return 0;

	// a page split is required if the page is full
	int split_required = !can_accomodate_tuple_insert(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, tuple_to_insert);
	
	// if a page split is required but the page split failed then return 0
	if(split_required && !split_page_at_cursor(pc_p))
		return 0;

	if(after_cursor)
	{

	}
	else
	{

	}

	insert_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, tuple_to_insert);

	return 1 + split_required;
}

int merge_page_at_cursor(page_cursor* pc_p);

int delete_tuple_at_cursor(page_cursor* pc_p)
{
	if(pc_p->lock_type != WRITER_LOCK)
		return 0;

	return 0;
}

void deinitialize_cursor(page_cursor* pc_p)
{
	release_lock(pc_p, pc_p->page);

	pc_p->page = NULL;
	pc_p->tuple = NULL;
	pc_p->page_id = NULL_PAGE_REFERENCE;
	pc_p->tuple_index = 0;

	pc_p->tpl_d = NULL;
	pc_p->dam_p = NULL;
}

void print_page_list(uint32_t page_list_head_page_id, const data_access_methods* dam_p);

void external_merge_sort_the_page_list(uint32_t page_list_head_page_id, uint16_t key_elements_count, const data_access_methods* dam_p);
