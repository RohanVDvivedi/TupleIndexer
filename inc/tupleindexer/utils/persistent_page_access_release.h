#ifndef PERSISTENT_PAGE_ACCESS_RELEASE_H
#define PERSISTENT_PAGE_ACCESS_RELEASE_H

#include<persistent_page_functions.h>

#include<page_access_methods_options.h>

#define WRITE_LOCK 1
#define READ_LOCK  0

// get new persistent page with write lock on it
// returns a NULL persistent_page on failure
persistent_page get_new_persistent_page_with_write_lock(const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// acquire appropriate lock type on page and get persistent page
// returns a NULL persistent_page on failure
persistent_page acquire_persistent_page_with_lock(const page_access_methods* pam_p, const void* transaction_id, uint64_t page_id, int lock_type, int* abort_error);

// downgrade writer lock on persistent page to reader lock
int downgrade_to_reader_lock_on_persistent_page(const page_access_methods* pam_p, const void* transaction_id, persistent_page* ppage, int opts, int* abort_error); // acceptable options : WAS_MODIFIED

// upgrade reader lock on persistent page to write lock
int upgrade_to_write_lock_on_persistent_page(const page_access_methods* pam_p, const void* transaction_id, persistent_page* ppage, int* abort_error);

// releases appropriate lock on page
int release_lock_on_persistent_page(const page_access_methods* pam_p, const void* transaction_id, persistent_page* ppage, int opts, int* abort_error); // acceptable options : if write locked then WAS_MODIFIED and FREE_PAGE, else only FREE_PAGE option is allowed

int free_persistent_page(const page_access_methods* pam_p, const void* transaction_id, uint64_t page_id, int* abort_error);

#endif