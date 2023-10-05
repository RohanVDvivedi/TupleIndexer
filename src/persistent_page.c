#include<persistent_page.h>

int is_persistent_page_NULL(const persistent_page* ppage, const data_access_methods* dam_p)
{
	return (ppage->page == NULL) && (ppage->page_id == dam_p->NULL_PAGE_ID);
}

int is_persistent_page_write_locked(const persistent_page* ppage);

int was_persistent_page_modified(const persistent_page* ppage);