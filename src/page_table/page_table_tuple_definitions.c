#include<page_table_tuple_definitions.h>

int init_bplus_tree_tuple_definitions(page_table_tuple_defs* pttd_p, uint32_t system_header_size, uint32_t page_size, uint8_t page_id_width, uint64_t NULL_PAGE_ID)
{
	// bytes required to store page id
	if(page_id_width == 0 || page_id_width > 8)
		return 0;

	// NULL_PAGE_ID must fit in page_id_width number of bytes
	if(page_id_width < 8 && NULL_PAGE_ID >= ( ((uint64_t)(1)) << (page_id_width * 8) ) )
		return 0;

	// there must be room for atleast some bytes after the page_table_page_header
	if(sizeof_PAGE_TABLE_PAGE_HEADER(bpttd_p) >= page_size)
		return 0;

	// initialize struct attributes
	pttd_p->NULL_PAGE_ID = NULL_PAGE_ID;
	pttd_p->page_id_width = page_id_width;
	pttd_p->page_size = page_size;

	pttd_p->system_header_size = system_header_size;
}

void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{

}

void print_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{

}