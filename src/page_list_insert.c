#include<page_list_insert.h>

#include<page_list_util.h>

int split_page_at_cursor(page_cursor* pc_p)
{
	if(pc_p->lock_type != WRITER_LOCK)
		return 0;

	// tuples on the page
	uint16_t tuple_count = get_tuple_count(pc_p->page);
	if(tuple_count <= 2)
		return 0;

	// tuples to transfer
	uint16_t tuples_to_transfer = tuple_count / 2;

	// tuples on old_page
	uint16_t tuples_on_old_page = tuple_count - tuples_to_transfer;

	// acquire a new page
	uint32_t new_page_id;
	void* new_page = acquire_new_page_with_lock(pc_p, &new_page_id);

		// find the next of the current page, and acquire lock on the next page
		uint32_t next_page_id = get_reference_page_id(pc_p->page, NEXT_PAGE_REFERENCE_INDEX);
		void* next_page = acquire_lock(pc_p, next_page_id);

			// set the next of the current page to the new_page_id
			set_reference_page_id(pc_p->page, NEXT_PAGE_REFERENCE_INDEX, new_page_id);

			// set the prev of the next page to the new_page_id
			set_reference_page_id(next_page, PREV_PAGE_REFERENCE_INDEX, new_page_id);

		// release lock on the the next page
		release_lock(pc_p, next_page);

		// insert tuples to initialize the new_page
		insert_tuples_from_page(new_page, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->page, tuple_count - tuples_to_transfer, tuple_count - 1);

		// re write and point the references to the correct pages for the newly added page
		set_reference_page_id(new_page, NEXT_PAGE_REFERENCE_INDEX, next_page_id);
		set_reference_page_id(new_page, PREV_PAGE_REFERENCE_INDEX, pc_p->page_id);

	// keep the cursor on the old page
	if(pc_p->tuple_id < tuples_on_old_page)
		release_lock(pc_p, new_page);
	else // move the cursor to the new page
	{
		release_lock(pc_p, pc_p->page);

		pc_p->page = new_page;
		pc_p->page_id = new_page_id;
		pc_p->tuple_id = pc_p->tuple_id - tuples_on_old_page;
	}

	return 1;
}

int insert_tuple_at_cursor(page_cursor* pc_p, int after_cursor, const void* tuple_to_insert)
{
	if(pc_p->lock_type != WRITER_LOCK)
		return 0;

	// a page split is required if the page is full, and it can not accomodate tuple
	int can_accomodate_tuple = can_accomodate_tuple_insert(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, tuple_to_insert);
	if(!can_accomodate_tuple)
	{
		// first try compaction
		reinsert_all_for_page_compaction(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d);

		can_accomodate_tuple = can_accomodate_tuple_insert(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, tuple_to_insert);

		// then try splitting the page_tuple
		// if the page pointed to by the cursor could not accomodate tuple
		//  and the page split failed then return 0
		if(!can_accomodate_tuple && !split_page_at_cursor(pc_p))
			return 0;
	}

	// before cursor
	uint32_t sct_si = pc_p->tuple_id;
	uint32_t sct_ei = get_tuple_count(pc_p->page) - 1;
	// after cursor
	if(after_cursor)
		sct_si += 1;

	if(sct_si <= sct_ei)
	{
		uint32_t sct_pd_id;
		void* sct_pd = acquire_new_page_with_lock(pc_p, &sct_pd_id);

			insert_tuples_from_page(sct_pd, pc_p->dam_p->page_size, pc_p->tpl_d, pc_p->page, sct_si, sct_ei);

			insert_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, tuple_to_insert);

			insert_tuples_from_page(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, sct_pd, 0, sct_ei - sct_si + 1);

		release_lock_and_free_page(pc_p, sct_pd);
	}
	else
		insert_tuple(pc_p->page, pc_p->dam_p->page_size, pc_p->tpl_d, tuple_to_insert);

	return 1;
}
