#include<page_table_tuple_definitions.h>

#include<page_table_page_header.h>

#include<page_layout_unaltered.h>

int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, uint32_t system_header_size, uint32_t page_size, uint8_t page_id_width, uint64_t NULL_PAGE_ID)
{
	// bytes required to store page id
	if(page_id_width == 0 || page_id_width > 8)
		return 0;

	// NULL_PAGE_ID must fit in page_id_width number of bytes
	if(page_id_width < 8 && NULL_PAGE_ID >= ( ((uint64_t)(1)) << (page_id_width * 8) ) )
		return 0;

	// initialize struct attributes
	pttd_p->NULL_PAGE_ID = NULL_PAGE_ID;
	pttd_p->page_id_width = page_id_width;
	pttd_p->page_size = page_size;

	pttd_p->system_header_size = system_header_size;

	// this can only be done after setting the above attributes
	// there must be room for atleast some bytes after the page_table_page_header
	if(!can_page_header_fit_on_page(sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), page_size))
		return 0;

	int res = 1;

	// initialize entry_def
	pttd_p->entry_def = get_new_tuple_def("temp_entry_def", 1, page_size);
	if(pttd_p->entry_def == NULL)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}
	res = insert_element_def(pttd_p->entry_def, "child_page_id", UINT, page_id_width, 1, &((user_value){.uint_value = pttd_p->NULL_PAGE_ID}));
	if(res == 0)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}
	res = finalize_tuple_def(pttd_p->entry_def);
	if(res == 0)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}

	// number of entries that can fir on the page
	pttd_p->entries_per_page = get_maximum_tuple_count_on_page(sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), page_size, &(pttd_p->entry_def->size_def));

	// there has to be atleast 2 entries per page for it to be a tree
	if(pttd_p->entries_per_page < 2)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}

	return 1;
}

void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{

}

void print_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{

}