#ifndef PERSISTENT_PAGE_H
#define PERSISTENT_PAGE_H

typedef struct persistent_page persistent_page;
struct persistent_page
{
	// id of the page
	uint64_t page_id;

	// page itself
	void* page;

	// it will basically store WAS_MODIFIED flag
	int flags;

	// will be set if page is write locked
	int is_write_locked;
};

// TODO - to be removed
#define get_persistent_page(page_id_v, page_v) ((const persistent_page){.page_id = page_id_v, .page = page_v})

// we need dam_p here, because that's what gives us NULL_PAGE_ID
// a persistent_page is NULL, if it's page = NULL and page_id == NULL_PAGE_ID
int is_persistent_page_NULL(const persistent_page* ppage, const data_access_methods* dam_p);

// if a persistent_page exists (i.e. is not NULL), then it always will be readable
// but the below function allows you to check if it is writable
int is_persistent_page_write_locked(const persistent_page* ppage);

// returns 1, if the WAS_MODIFIED bit in the flags of persistent_page is set
int was_persistent_page_modified(const persistent_page* ppage);

#endif