#include<persistent_page_access_release.h>

#include<page_access_methods.h>

#include<stdlib.h>

persistent_page get_new_persistent_page_with_write_lock(const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// no new locks can be issued, or modified, once a transaction is aborted
	if(*(abort_error))
	{
		printf("BUG :: attempting to acquire new page with a write lock, after knowing of an abort\n");
		exit(-1);
	}

	persistent_page ppage = {};
	ppage.page = pam_p->get_new_page_with_write_lock(pam_p->context, transaction_id, &(ppage.page_id), abort_error);

	// if could not allocate a page on disk then just return a NULL persistent page
	if(ppage.page == NULL)
		return get_NULL_persistent_page(pam_p);

	// set the write locked flag on ppage
	ppage.flags = 0;
	ppage.is_write_locked = 1;

	return ppage;
}

persistent_page acquire_persistent_page_with_lock(const page_access_methods* pam_p, const void* transaction_id, uint64_t page_id, int lock_type, int* abort_error)
{
	// no new locks can be issued, or modified, once a transaction is aborted
	if(*(abort_error))
	{
		printf("BUG :: attempting to acquire page lock, after knowing of an abort\n");
		exit(-1);
	}

	persistent_page ppage = {.page_id = page_id};

	switch(lock_type)
	{
		case READ_LOCK :
		{
			ppage.page = pam_p->acquire_page_with_reader_lock(pam_p->context, transaction_id, ppage.page_id, abort_error);

			if(ppage.page == NULL)
				return get_NULL_persistent_page(pam_p);

			ppage.flags = 0;
			ppage.is_write_locked = 0;

			break;
		}
		case WRITE_LOCK :
		{
			ppage.page = pam_p->acquire_page_with_writer_lock(pam_p->context, transaction_id, ppage.page_id, abort_error);

			if(ppage.page == NULL)
				ppage = get_NULL_persistent_page(pam_p);

			ppage.flags = 0;
			ppage.is_write_locked = 1;

			break;
		}
	}

	return ppage;
}

int downgrade_to_reader_lock_on_persistent_page(const page_access_methods* pam_p, const void* transaction_id, persistent_page* ppage, int opts, int* abort_error)
{
	// no new locks can be issued, or modified, once a transaction is aborted
	if(*(abort_error))
	{
		printf("BUG :: attempting to downgrade page lock, after knowing of an abort\n");
		exit(-1);
	}

	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to downgrade a reader lock on page\n");
		exit(-1);
	}

	int res = pam_p->downgrade_writer_lock_to_reader_lock_on_page(pam_p->context, transaction_id, ppage->page, opts | ppage->flags, abort_error);

	if(res)
	{
		ppage->flags = 0;
		ppage->is_write_locked = 0;
	}

	return res;
}

int upgrade_to_write_lock_on_persistent_page(const page_access_methods* pam_p, const void* transaction_id, persistent_page* ppage, int* abort_error)
{
	// no new locks can be issued, or modified, once a transaction is aborted
	if(*(abort_error))
	{
		printf("BUG :: attempting to upgrade page lock, after knowing of an abort\n");
		exit(-1);
	}

	if(is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to upgrade a write lock on page\n");
		exit(-1);
	}

	int res = pam_p->upgrade_reader_lock_to_writer_lock_on_page(pam_p->context, transaction_id, ppage->page, abort_error);

	if(res)
	{
		ppage->flags = 0;
		ppage->is_write_locked = 1;
	}

	return res;
}

int release_lock_on_persistent_page(const page_access_methods* pam_p, const void* transaction_id, persistent_page* ppage, int opts, int* abort_error)
{
	// a page can not be freed, once a transaction is aborted
	if(*(abort_error))
	{
		if((ppage->flags | opts) & FREE_PAGE)
		{
			printf("BUG :: attempting to free a page, while releasing a lock, after knowing of an abort\n");
			exit(-1);
		}
	}

	// cache the old_abort_error
	int old_abort_error = (*abort_error);

	int res = 0;

	// release lock appropriately
	if(is_persistent_page_write_locked(ppage))
		res = pam_p->release_writer_lock_on_page(pam_p->context, transaction_id, ppage->page, opts | ppage->flags, abort_error);
	else
		res = pam_p->release_reader_lock_on_page(pam_p->context, transaction_id, ppage->page, opts | ppage->flags, abort_error);

	// this means, we failed to release a lock on a page, after the abort
	// this must not happen, unless there is a double free bug in TupleIndexer OR faulty pam implementation
	if(old_abort_error && !res)
	{
		printf("BUG :: release_*_lock_on_page() (without FREE_PAGE flag) failed after an abort, (possible double free in TupleIndexer OR faulty pam implementation)\n");
		exit(-1);
	}

	// if successfull in releasing lock, then set ppage to NULL persistent_page
	if(res)
		(*ppage) = get_NULL_persistent_page(pam_p);

	return res;
}

int free_persistent_page(const page_access_methods* pam_p, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	// a page can not be freed, once a transaction is aborted
	if(*(abort_error))
	{
		printf("BUG :: attempting to free a page, after knowing of an abort\n");
		exit(-1);
	}

	return pam_p->free_page(pam_p->context, transaction_id, page_id, abort_error);
}