#include<persistent_page_access_release.h>

#include<data_access_methods.h>

static persistent_page get_NULL_persistent_page(const data_access_methods* dam_p)
{
	return (persistent_page){.page = NULL, .page_id = dam_p->NULL_PAGE_ID};
}

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

			ppage.flags = WAS_MODIFIED;
			ppage.is_write_locked = 1;				

			break;
		}
	}

	return ppage;
}

// downgrade writer lock on persistent page to reader lock
int downgrade_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage, int opts); // acceptable options : WAS_MODIFIED and FORCE_FLUSH

// upgrade reader lock on persistent page to write lock
int upgrade_to_write_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage);

// releases appropriate lock on page
int release_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage, int opts); // acceptable options : if write locked then WAS_MODIFIED, FORCE_FLUSH and FREE_PAGE, else only FREE_PAGE option is allowed

int free_persistent_page(const data_access_methods* dam_p, uint64_t page_id)
{
	return dam_p->free_page(dam_p->context, page_id);
}