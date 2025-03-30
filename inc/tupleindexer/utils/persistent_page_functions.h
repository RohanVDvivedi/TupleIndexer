#ifndef PERSISTENT_PAGE_FUNCTIONS_H
#define PERSISTENT_PAGE_FUNCTIONS_H

#include<opaque_page_access_methods.h>

#include<opaque_page_modification_methods.h>

#include<persistent_page.h>

// we need pam_p here, because that's what gives us NULL_PAGE_ID
// a persistent_page is NULL, if it's page = NULL and page_id == NULL_PAGE_ID
int is_persistent_page_NULL(const persistent_page* ppage, const page_access_methods* pam_p);

// to get a NULL persistent_page i.e. persistent_page with its .page = NULL and .page_id = NULL_PAGE_ID
persistent_page get_NULL_persistent_page(const page_access_methods* pam_p);

// if a persistent_page exists (i.e. is not NULL), then it always will be readable
// but the below function allows you to check if it is writable
int is_persistent_page_write_locked(const persistent_page* ppage);

// returns 1, if the WAS_MODIFIED bit in the flags of persistent_page is set
int was_persistent_page_modified(const persistent_page* ppage);

#include<persistent_page_unaltered.h>

#include<persistent_page_unaltered_util.h>

#include<persistent_page_altered.h>

#include<persistent_page_altered_util.h>

#include<persistent_page_access_release.h>

#endif