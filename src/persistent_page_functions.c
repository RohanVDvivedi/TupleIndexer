#include<persistent_page_functions.h>

#include<data_access_methods.h>

#include<stddef.h>

int is_persistent_page_NULL(const persistent_page* ppage, const data_access_methods* dam_p)
{
	return (ppage->page == NULL) && (ppage->page_id == dam_p->NULL_PAGE_ID);
}

int is_persistent_page_write_locked(const persistent_page* ppage)
{
	return ppage->is_write_locked;
}

int was_persistent_page_modified(const persistent_page* ppage)
{
	return !!(ppage->flags | WAS_MODIFIED);
}