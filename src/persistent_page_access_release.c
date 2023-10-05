#include<persistent_page_access_release.h>

#include<data_access_methods.h>

static persistent_page get_NULL_persistent_page(const data_access_methods* dam_p)
{
	return (persistent_page){.page = NULL, .page_id = NULL_PAGE_ID};
}

persistent_page get_new_persistent_page_with_write_lock(const data_access_methods* dam_p);

// acquire appropriate lock type on page and get persistent page
persistent_page acquire_persistent_page_with_lock(const data_access_methods* dam_p, uint64_t page_id, int lock_type);

// downgrade writer lock on persistent page to reader lock
int downgrade_lock_on_persistent_page(const data_access_methods* dam_p, persustent_page* ppage, int opts); // acceptable options : WAS_MODIFIED and FORCE_FLUSH

// upgrade reader lock on persistent page to write lock
int upgrade_to_write_lock_on_persistent_page(const data_access_methods* dam_p, persustent_page* ppage);

// releases appropriate lock on page
int release_lock_on_persistent_page(const data_access_methods* dam_p, persustent_page* ppage, int opts); // acceptable options : if write locked then WAS_MODIFIED, FORCE_FLUSH and FREE_PAGE, else only FREE_PAGE option is allowed

int free_persistent_page(const data_access_methods* dam_p, uint64_t page_id)
{
	return dam_p->free_page(dam_p->context, page_id);
}