#include<page_list.h>

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

void initialize_cursor(page_cursor* pc_p, page_cursor_lock_type lock_type, page_cursor_traversal_direction traverse_dir, uint32_t page_list_page_id, const tuple_def* tpl_d, const data_access_methods* dam_p)
{
	pc_p->lock_type = lock_type;
	pc_p->traverse_dir = traverse_dir;

	pc_p->tpl_d = tpl_d;
	pc_p->dam_p = dam_p;

	pc_p->page = acquire_lock(pc_p, page_list_page_id);
	pc_p->page_id = page_list_page_id;
}

void deinitialize_cursor(page_cursor* pc_p)
{
	release_lock(pc_p, pc_p->page);

	pc_p->page = NULL;
	pc_p->page_id = NULL_PAGE_REFERENCE;

	pc_p->tpl_d = NULL;
	pc_p->dam_p = NULL;
}

// TODO :: This function will be removed in the future
#include<in_memory_data_store.h>
void print_page_list_debug(uint32_t page_list_head_page_id, const memory_store_context* cntxt, uint32_t page_size, const tuple_def* tpl_d)
{
	printf("\nPAGE LIST\n\n");
	uint32_t next_page_id = page_list_head_page_id;
	while(next_page_id != NULL_PAGE_REFERENCE)
	{
		printf("page_id : %u\n", next_page_id);
		void* page = get_page_for_debug(cntxt, next_page_id);
		print_page(page, page_size, tpl_d);
		printf("\n\n");
		next_page_id = get_reference_page_id(page, NEXT_PAGE_REFERENCE_INDEX);
	}
	printf("--xx--\n\n");
}
