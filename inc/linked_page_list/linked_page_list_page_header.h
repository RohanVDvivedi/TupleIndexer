#ifndef LINKED_PAGE_LIST_PAGE_HEADER_H
#define LINKED_PAGE_LIST_PAGE_HEADER_H

#include<common_page_header.h>
#include<linked_page_list_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page.h>

typedef struct linked_page_list_page_header linked_page_list_page_header;
struct linked_page_list_page_header
{
	common_page_header parent;

	// linked_page_list is a circular linked_list of pages

	// page_id of next page of this page
	uint64_t next_page_id;

	// page_id of previous page of this page
	uint64_t prev_page_id;
};

#define sizeof_LINKED_PAGE_LIST_PAGE_HEADER get_offset_to_end_of_linked_page_list_page_header

uint32_t get_offset_to_end_of_linked_page_list_page_header(const linked_page_list_tuple_defs* lpltd_p);

uint64_t get_next_page_id_of_linked_page_list_page(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p);

uint64_t get_prev_page_id_of_linked_page_list_page(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p);

linked_page_list_page_header get_linked_page_list_page_header(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p);

void serialize_linked_page_list_page_header(void* hdr_serial, const linked_page_list_page_header* lplph_p, const linked_page_list_tuple_defs* lpltd_p);

void set_linked_page_list_page_header(persistent_page* ppage, const linked_page_list_page_header* lplph_p, const linked_page_list_tuple_defs* lpltd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_linked_page_list_page_header(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p);

#endif