#include<page_list.h>

static inline void* acquire_lock(page_cursor* pc_p, uint32_t page_id)
{
	switch(pc_p->lock_type)
	{
		case READER_LOCK :
			return pc_p->dam_p->acquire_page_with_reader_lock(pc_p->dam_p->context, page_id);
		case WRITER_LOCK :
			return pc_p->dam_p->acquire_page_with_writer_lock(pc_p->dam_p->context, page_id);
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
	set_reference_page_id(new_page_list_head_page, NEXT_PAGE_REF_INDEX, NULL_REF);
	set_reference_page_id(new_page_list_head_page, PREV_PAGE_REF_INDEX, NULL_REF);
	
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
	pc_p->page_id = NULL_REF;

	pc_p->tpl_d = NULL;
	pc_p->dam_p = NULL;
}

void* get_page_of_cursor(page_cursor* pc_p)
{
	return pc_p->page;
}

int seek_to_next(page_cursor* pc_p)
{
	if(pc_p->traverse_dir != NEXT_PAGE_DIR)
		return 0;

	if(pc_p->page_id == NULL_REF)
		return 0;

	uint32_t next_page_id = get_reference_page_id(pc_p->page, NEXT_PAGE_REF_INDEX);

	if(next_page_id == NULL_REF)
		return 0;

	void* next_page = acquire_lock(pc_p, next_page_id);
	release_lock(pc_p, pc_p->page);

	pc_p->page = next_page;

	return 1;
}

int seek_to_prev(page_cursor* pc_p)
{
	if(pc_p->traverse_dir != PREV_PAGE_DIR)
		return 0;

	if(pc_p->page_id == NULL_REF)
		return 0;

	uint32_t prev_page_id = get_reference_page_id(pc_p->page, PREV_PAGE_REF_INDEX);

	if(prev_page_id == NULL_REF)
		return 0;

	void* prev_page = acquire_lock(pc_p, prev_page_id);
	release_lock(pc_p, prev_page);

	pc_p->page = prev_page;

	return 1;
}

int split_towards_next(page_cursor* pc_p, uint16_t next_tuple_count)
{
	if(pc_p->lock_type != WRITER_LOCK)
		return 0;

	if(pc_p->traverse_dir != NEXT_PAGE_DIR)
		return 0;

	if(pc_p->page_id == NULL_REF)
		return 0;

	uint32_t tuple_count = get_tuple_count(pc_p->page);

	// we must not leave any page empty
	if(tuple_count == 0 || next_tuple_count >= tuple_count)
		return 0;

	uint32_t new_page_id;
	void* new_page = acquire_new_page_with_lock(pc_p, &new_page_id);

		// if there exist an old next page, we need to take locks and fix its prev_page
		uint32_t next_page_id = get_reference_page_id(pc_p->page, NEXT_PAGE_REF_INDEX);
		if(next_page_id != NULL_REF)
		{
			void* next_page = acquire_lock(pc_p, next_page_id);
				set_reference_page_id(next_page, PREV_PAGE_REF_INDEX, new_page_id);
			release_lock(pc_p, next_page);
		}

		// build the new_page, that will become the new next_page for thr current page
		init_page(new_page, pc_p->dam_p->page_size, 1, 2, NULL);
		set_reference_page_id(new_page, NEXT_PAGE_REF_INDEX, next_page_id);
		set_reference_page_id(new_page, PREV_PAGE_REF_INDEX, pc_p->page_id);

		// insert tuples from current page to the new_page (the new next page)
		uint16_t tuples_copied = insert_tuples_from_page(new_page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->page, tuple_count - next_tuple_count, tuple_count - 1);

	release_lock(pc_p, new_page);

	// delete tuples that were copied to the new_page (the new next_page)
	while(tuples_copied > 0)
	{
		delete_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, get_tuple_count(pc_p->page) - 1);
		tuples_copied--;
	}

	// now we fix its next_page of the current page
	set_reference_page_id(pc_p->page, NEXT_PAGE_REF_INDEX, new_page_id);

	return 1;
}

int split_towards_prev(page_cursor* pc_p, uint16_t prev_tuple_count)
{
	if(pc_p->lock_type != WRITER_LOCK)
		return 0;

	if(pc_p->traverse_dir != PREV_PAGE_DIR)
		return 0;

	if(pc_p->page_id == NULL_REF)
		return 0;

	uint32_t tuple_count = get_tuple_count(pc_p->page);

	// we must not leave any page empty
	if(tuple_count == 0 || prev_tuple_count >= tuple_count)
		return 0;

	uint32_t new_page_id;
	void* new_page = acquire_new_page_with_lock(pc_p, &new_page_id);

		// if there exist an old prev page, we need to take locks and fix its next_page
		uint32_t prev_page_id = get_reference_page_id(pc_p->page, PREV_PAGE_REF_INDEX);
		if(prev_page_id != NULL_REF)
		{
			void* prev_page = acquire_lock(pc_p, prev_page_id);
				set_reference_page_id(prev_page, NEXT_PAGE_REF_INDEX, new_page_id);
			release_lock(pc_p, prev_page);
		}

		// build the new_page, that will become the new next_page for thr current page
		init_page(new_page, pc_p->dam_p->page_size, 1, 2, NULL);
		set_reference_page_id(new_page, NEXT_PAGE_REF_INDEX, pc_p->page_id);
		set_reference_page_id(new_page, PREV_PAGE_REF_INDEX, prev_page_id);

		// insert tuples from current page to the new_page (the new prev page)
		uint16_t tuples_copied = insert_tuples_from_page(new_page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->page, 0, prev_tuple_count - 1);

	release_lock(pc_p, new_page);

	// delete tuples that were copied to the new_page (the new prev_page)
	uint16_t delete_tuple_at = 0;
	uint16_t tuples_deleted = 0;
	while(tuples_copied > 0)
	{
		if(exists_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, delete_tuple_at))
		{
			delete_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, delete_tuple_at);
			tuples_copied--;
			tuples_deleted++;
		}
		else
			delete_tuple_at++;
	}

	// since we deleted the tuples from the top, we may have created holes in the page
	// we fix that using re insertion of all the tuples
	if(tuples_deleted > ((tuple_count / 3) + 2))
		reinsert_all_for_page_compaction(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d);

	// now we fix its prev_page of the current page
	set_reference_page_id(pc_p->page, PREV_PAGE_REF_INDEX, new_page_id);

	return 1;
}


// TODO :: This function will be removed in the future
#include<in_memory_data_store.h>
void print_page_list_debug(uint32_t page_list_head_page_id, const memory_store_context* cntxt, uint32_t page_size, const tuple_def* tpl_d)
{
	printf("\nPAGE LIST\n\n");
	uint32_t next_page_id = page_list_head_page_id;
	while(next_page_id != NULL_REF)
	{
		printf("page_id : %u\n", next_page_id);
		void* page = get_page_for_debug(cntxt, next_page_id);
		print_page(page, page_size, tpl_d);
		printf("\n\n");
		next_page_id = get_reference_page_id(page, NEXT_PAGE_REF_INDEX);
	}
	printf("--xx--\n\n");
}
