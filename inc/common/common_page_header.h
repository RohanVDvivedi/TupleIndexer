#ifndef COMMON_PAGE_HEADER_H
#define COMMON_PAGE_HEADER_H

#include<page_access_specification.h>
#include<opaque_page_modification_methods.h>

#include<persistent_page.h>
#include<page_type.h>

#include<stdint.h>


typedef struct common_page_header common_page_header;
struct common_page_header
{
	page_type type;
};

/*
**		system header is prefix to even the common page header
**		size of this system header is equal to the what was specified in bplus_tree_tuple_definitions object
*/

uint32_t get_offset_to_end_of_common_page_header(const page_access_specs* pas_p);

common_page_header get_common_page_header(const persistent_page* ppage, const page_access_specs* pas_p);

void serialize_common_page_header(void* hdr_serial, const common_page_header* cph_p, const page_access_specs* pas_p);

void set_common_page_header(persistent_page* ppage, const common_page_header* cph_p, const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_common_page_header(const persistent_page* ppage, const page_access_specs* pas_p);

#endif