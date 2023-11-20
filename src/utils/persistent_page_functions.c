#include<persistent_page_functions.h>

#include<page_access_methods.h>

#include<stddef.h>

int is_persistent_page_NULL(const persistent_page* ppage, const page_access_methods* dam_p)
{
	return (ppage->page == NULL) && (ppage->page_id == dam_p->pas.NULL_PAGE_ID);
}

persistent_page get_NULL_persistent_page(const page_access_methods* dam_p)
{
	return (persistent_page){.page = NULL, .page_id = dam_p->pas.NULL_PAGE_ID};
}

int is_persistent_page_write_locked(const persistent_page* ppage)
{
	return ppage->is_write_locked;
}

int was_persistent_page_modified(const persistent_page* ppage)
{
	return !!(ppage->flags | WAS_MODIFIED);
}