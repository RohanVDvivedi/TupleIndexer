#include<page_table_tuple_definitions.h>

#include<page_table_page_header.h>

#include<persistent_page_functions.h>

int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p)
{
	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs fo the bpttd
	pttd_p->pas_p = pas_p;

	// this can only be done after setting the pas_p attribute of pttd
	// there must be room for atleast some bytes after the page_table_page_header
	if(!can_page_header_fit_on_persistent_page(sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), pttd_p->pas_p->page_size))
		return 0;

	int res = 1;

	// initialize entry_def
	pttd_p->entry_def = get_new_tuple_def("temp_entry_def", 1, pttd_p->pas_p->page_size);
	if(pttd_p->entry_def == NULL)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}
	res = insert_element_def(pttd_p->entry_def, "child_page_id", UINT, pttd_p->pas_p->page_id_width, 1, &((user_value){.uint_value = pttd_p->pas_p->NULL_PAGE_ID}));
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
	pttd_p->entries_per_page = get_maximum_tuple_count_on_page(sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def));

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
	if(pttd_p->entry_def)
		delete_tuple_def(pttd_p->entry_def);

	pttd_p->pas_p = NULL;
	pttd_p->entry_def = NULL;
	pttd_p->entries_per_page = 0;
}

void print_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{
	printf("Page_table tuple defs:\n");

	if(pttd_p->pas_p)
		print_page_access_specs(pttd_p->pas_p);

	printf("entry_def = ");
	if(pttd_p->entry_def)
		print_tuple_def(pttd_p->entry_def);
	else
		printf("NULL\n");

	printf("entries_per_page = %"PRIu64"\n", pttd_p->entries_per_page);
}