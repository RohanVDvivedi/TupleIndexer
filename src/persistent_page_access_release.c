#include<persistent_page_access_release.h>

#include<data_access_methods.h>

#include<stdlib.h>

persistent_page get_new_persistent_page_with_write_lock(const data_access_methods* dam_p)
{
	persistent_page ppage = {};
	ppage.page = dam_p->get_new_page_with_write_lock(dam_p->context, &(ppage.page_id));

	// if could not allocate a page on disk then just return a NULL persistent page
	if(ppage.page == NULL)
		return get_NULL_persistent_page(dam_p);

	// set the write locked flag on ppage
	ppage.flags = 0;
	ppage.is_write_locked = 1;

	return ppage;
}

persistent_page acquire_persistent_page_with_lock(const data_access_methods* dam_p, uint64_t page_id, int lock_type)
{
	persistent_page ppage = {.page_id = page_id};

	switch(lock_type)
	{
		case READ_LOCK :
		{
			ppage.page = dam_p->acquire_page_with_reader_lock(dam_p->context, ppage.page_id);

			if(ppage.page == NULL)
				return get_NULL_persistent_page(dam_p);

			ppage.flags = 0;
			ppage.is_write_locked = 0;

			break;
		}
		case WRITE_LOCK :
		{
			ppage.page = dam_p->acquire_page_with_writer_lock(dam_p->context, ppage.page_id);

			if(ppage.page == NULL)
				ppage = get_NULL_persistent_page(dam_p);

			ppage.flags = 0;
			ppage.is_write_locked = 1;				

			break;
		}
	}

	return ppage;
}

int downgrade_to_reader_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage, int opts)
{
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to downgrade a reader lock on page\n");
		exit(-1);
	}

	int res = dam_p->downgrade_writer_lock_to_reader_lock_on_page(dam_p->context, ppage->page, opts | ppage->flags);

	if(res)
	{
		ppage->flags = 0;
		ppage->is_write_locked = 0;
	}

	return res;
}

int upgrade_to_write_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage)
{
	if(is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to upgrade a write lock on page\n");
		exit(-1);
	}

	int res = dam_p->upgrade_reader_lock_to_writer_lock_on_page(dam_p->context, ppage->page);

	if(res)
	{
		ppage->flags = 0;
		ppage->is_write_locked = 1;
	}

	return res;
}

int release_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage, int opts)
{
	int res = 0;

	// release lock appropriately
	if(is_persistent_page_write_locked(ppage))
		res = dam_p->release_writer_lock_on_page(dam_p->context, ppage->page, opts | ppage->flags);
	else
		res = dam_p->release_reader_lock_on_page(dam_p->context, ppage->page, opts | ppage->flags);

	// if successfull in releasing lock, then set ppage to NULL persistent_page
	if(res)
		(*ppage) = get_NULL_persistent_page(dam_p);

	return res;
}

int free_persistent_page(const data_access_methods* dam_p, uint64_t page_id)
{
	return dam_p->free_page(dam_p->context, page_id);
}