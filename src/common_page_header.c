#include<common_page_header.h>

#include<page_layout.h>

page_type get_type_of_page(const void* page, uint32_t page_size)
{
	return (page_type)(((const page_header*)get_page_header((void*)page, page_size))->type);
}

void set_type_of_page(void* page, uint32_t page_size, page_type type)
{
	((page_header*)get_page_header(page, page_size))->type = type;
}

void print_common_page_header(const void* page, uint32_t page_size)
{
	printf("page_type : %s\n", page_type_string[get_type_of_page(page, page_size)]);
}