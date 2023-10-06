#ifndef PERSISTENT_PAGE_ACCESS_RELEASE_H
#define PERSISTENT_PAGE_ACCESS_RELEASE_H

#include<persistent_page_functions.h>

#include<data_access_methods_options.h>

#define WRITE_LOCK 1
#define READ_LOCK  0

// get new persistent page with write lock on it
// returns a NULL persistent_page on failure
persistent_page get_new_persistent_page_with_write_lock(const data_access_methods* dam_p);

// acquire appropriate lock type on page and get persistent page
// returns a NULL persistent_page on failure
persistent_page acquire_persistent_page_with_lock(const data_access_methods* dam_p, uint64_t page_id, int lock_type);

// downgrade writer lock on persistent page to reader lock
int downgrade_to_reader_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage, int opts); // acceptable options : WAS_MODIFIED and FORCE_FLUSH

// upgrade reader lock on persistent page to write lock
int upgrade_to_write_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage);

// releases appropriate lock on page
int release_lock_on_persistent_page(const data_access_methods* dam_p, persistent_page* ppage, int opts); // acceptable options : if write locked then WAS_MODIFIED, FORCE_FLUSH and FREE_PAGE, else only FREE_PAGE option is allowed

int free_persistent_page(const data_access_methods* dam_p, uint64_t page_id);

#endif