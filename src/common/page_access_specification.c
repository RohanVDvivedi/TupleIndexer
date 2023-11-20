#include<page_access_specification.h>

#include<page_layout_unaltered.h>

int is_valid_page_access_specs(const page_access_specs* pas_p)
{
	// bytes required to store page id, must be between 1 and 8 both inclusive
	if(pas_p->page_id_width == 0 || pas_p->page_id_width > 8)
		return 0;

	// NULL_PAGE_ID must fit in page_id_width number of bytes
	if(pas_p->page_id_width < 8 && pas_p->NULL_PAGE_ID >= ( ((uint64_t)(1)) << (pas_p->page_id_width * 8) ) )
		return 0;

	// if the page can not even fit the system header on the page, then fail
	if(!can_page_header_fit_on_page(pas_p->system_header_size, pas_p->page_size))
		return 0;

	return 1;
}