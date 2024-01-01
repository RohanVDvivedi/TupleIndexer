#include<linked_page_list_page_header.h>

uint32_t get_offset_to_end_of_linked_page_list_page_header(const linked_page_list_tuple_defs* lpltd_p)
{
	return get_offset_to_end_of_common_page_header(bpttd_p->pas_p) + (2 * lpltd_p->pas_p->page_id_width);
}

uint32_t get_prev_page_id_of_linked_page_list_page(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p)
{
	return get_linked_page_list_page_header(ppage, lpltd_p).prev_page_id;
}

uint64_t get_next_page_id_of_linked_page_list_page(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p);
{
	return get_linked_page_list_page_header(ppage, lpltd_p).next_page_id;
}

linked_page_list_page_header get_linked_page_list_page_header(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p);

void serialize_linked_page_list_page_header(void* hdr_serial, const linked_page_list_page_header* lplph_p, const linked_page_list_tuple_defs* lpltd_p);

void set_linked_page_list_page_header(persistent_page* ppage, const linked_page_list_page_header* lplph_p, const linked_page_list_tuple_defs* lpltd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_linked_page_list_page_header(const persistent_page* ppage, const linked_page_list_tuple_defs* lpltd_p);