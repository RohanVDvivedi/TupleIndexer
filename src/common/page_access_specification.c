#include<page_access_specification.h>

#include<page_layout_unaltered.h>

int init_page_access_specs(page_access_specs* pas_p, uint8_t page_id_width, uint32_t page_size, uint64_t NULL_PAGE_ID, uint32_t system_header_size)
{
	// bytes required to store page id, must be between 1 and 8 both inclusive
	if(page_id_width == 0 || page_id_width > 8)
		return 0;

	// NULL_PAGE_ID must fit in page_id_width number of bytes
	if(page_id_width < 8 && NULL_PAGE_ID >= ( ((uint64_t)(1)) << (page_id_width * 8) ) )
		return 0;

	// if the page can not even fit the system header on the page, then fail
	if(!can_page_header_fit_on_page(system_header_size, page_size))
		return 0;

	pas_p->page_id_width = page_id_width;
	pas_p->page_size = page_size;
	pas_p->NULL_PAGE_ID = NULL_PAGE_ID;
	pas_p->system_header_size = system_header_size;

	return 1;
}