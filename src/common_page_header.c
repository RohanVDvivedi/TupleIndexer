#include<common_page_header.h>

#include<page_layout.h>
#include<int_accesses.h>

const char* page_type_string[] = {
	"BPLUS_TREE_LEAF_PAGE",
	"BPLUS_TREE_INTERIOR_PAGE",
};

// allowed values for size for storing page_type = 1 or 2
#define BYTES_FOR_PAGE_TYPE 2

uint32_t get_offset_of_page_type_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return bpttd_p->default_common_header_size;
}

uint32_t get_size_of_page_type_header()
{
	return BYTES_FOR_PAGE_TYPE;
}

page_type get_type_of_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* page_type_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_page_type_header(bpttd_p);
	return (page_type)read_uint16(page_type_header, BYTES_FOR_PAGE_TYPE);
}

void set_type_of_page(void* page, page_type type, const bplus_tree_tuple_defs* bpttd_p)
{
	void* page_type_header = get_page_header((void*)page, bpttd_p->page_size) + get_offset_of_page_type_header(bpttd_p);
	write_uint16(page_type_header, BYTES_FOR_PAGE_TYPE, ((uint16_t)type));
}

void print_common_page_header(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	printf("page_type : %s\n", page_type_string[get_type_of_page(page, bpttd_p)]);
}