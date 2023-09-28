#ifndef PERSISTENT_PAGE_H
#define PERSISTENT_PAGE_H

typedef struct persistent_page persistent_page;
struct persistent_page
{
	// id of the page
	uint64_t page_id;

	// page itself
	void* page;
};

#endif