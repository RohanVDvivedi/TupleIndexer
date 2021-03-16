#ifndef PAGE_LIST_UTIL_H
#define PAGE_LIST_UTIL_H

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

#endif