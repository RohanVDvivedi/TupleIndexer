#include<page_list.h>

static int is_head_page(page_list* pl_p, uint32_t page_id)
{
	return (pl_p->head_page_id != NULL_PAGE_REFERENCE) && (pl_p->head_page_id == page_id);
}

int initialize_page_list(page_list* pl_p, uint32_t head_page_id, uint32_t tail_page_id, tuple_def* tpl_d)
{
	initialize_rwlock(&(pl_p->page_list_lock));
	pl_p->head_page_id = head_page_id;
	pl_p->tail_page_id = tail_page_id;
	pl_p->tuple_definition = tpl_d;
}

int insert_in_page_list(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p)
{
	int inserted = 0;
	write_lock(&(pl_p->page_list_lock));

	void* old_tail_page = NULL;

	if(pl_p->tail_page_id != NULL_PAGE_REFERENCE)
	{
		void* tail_page = dam_p->acquire_page_with_writer_lock(dam_p->context, pl_p->tail_page_id);
		inserted = insert_tuple(tail_page, dam_p->page_size, pl_p->tuple_definition, tuple_like);

		if(inserted)
			dam_p->release_writer_lock_on_page(dam_p->context, tail_page);
		else
			old_tail_page = tail_page;
	}

	if(!inserted)
	{
		uint32_t new_page_id = NULL_PAGE_REFERENCE;
		void* new_page = dam_p->get_new_page_with_write_lock(dam_p->context, &new_page_id);

		int initialized = init_page(new_page, dam_p->page_size, 0, 2, pl_p->tuple_definition);
		if(initialized)
		{
			inserted = insert_tuple(new_page, dam_p->page_size, pl_p->tuple_definition, tuple_like);

			set_reference_page_id(new_page, NEXT_PAGE_REFERENCE_INDEX, NULL_PAGE_REFERENCE);
			set_reference_page_id(new_page, PREV_PAGE_REFERENCE_INDEX, pl_p->tail_page_id);

			dam_p->release_writer_lock_on_page(dam_p->context, new_page);

			pl_p->tail_page_id = new_page_id;
			if(pl_p->head_page_id == NULL_PAGE_REFERENCE)
				pl_p->head_page_id = new_page_id;
		}
		else
			dam_p->release_writer_lock_and_free_page(dam_p->context, new_page);

		if(old_tail_page != NULL)
		{
			set_reference_page_id(old_tail_page, NEXT_PAGE_REFERENCE_INDEX, new_page_id);
			dam_p->release_writer_lock_on_page(dam_p->context, old_tail_page);
		}
	}

	write_unlock(&(pl_p->page_list_lock));
	return inserted;
}

int deinitialize_page_list(page_list* pl_p)
{
	deinitialize_rwlock(&(pl_p->page_list_lock));
}

int initialize_cursor(page_cursor* pc_p, page_list* pl_p, const data_access_methods* dam_p);

int delete_tuple_at_the_cursor(page_list* pl_p, const void* tuple_like, const data_access_methods* dam_p);

int seek_to_next_tuple_in_page_list(page_cursor* pc_p, const data_access_methods* dam_p);

int deinitialize_cursor(page_cursor* pc_p);

void external_merge_sort_the_page_list(page_list* pl_p, uint16_t key_elements_count, const data_access_methods* dam_p);
