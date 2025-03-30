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

#include<tupleindexer/utils/page_lock_type.h>

#endif