#ifndef PERSISTENT_PAGE_H
#define PERSISTENT_PAGE_H

#include<stdint.h>

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

#endif