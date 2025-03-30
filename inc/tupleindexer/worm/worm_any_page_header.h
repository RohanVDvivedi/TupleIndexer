#ifndef WORM_ANY_PAGE_HEADER_H
#define WORM_ANY_PAGE_HEADER_H

#include<common_page_header.h>
#include<worm_tuple_definitions.h>
#include<opaque_page_modification_methods.h>
#include<persistent_page_functions.h>

#include<serial_int.h>

#include<stdlib.h>

typedef struct worm_any_page_header worm_any_page_header;
struct worm_any_page_header
{
	common_page_header parent;

	// all pages including the head are singly linked, with pointers (page_id-s) to next page
	uint64_t next_page_id; // could be NULL_PAGE_ID if it is the tail
};

#define sizeof_WORM_ANY_PAGE_HEADER get_offset_to_end_of_worm_any_page_header

static inline uint32_t get_offset_to_end_of_worm_any_page_header(const worm_tuple_defs* wtd_p);

static inline uint64_t get_next_page_id_of_worm_any_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

static inline worm_any_page_header get_worm_any_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

static inline void serialize_worm_any_page_header(void* hdr_serial, const worm_any_page_header* waph_p, const worm_tuple_defs* wtd_p);

static inline void set_worm_any_page_header(persistent_page* ppage, const worm_any_page_header* whph_p, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// prints header of worm any page
static inline void print_worm_any_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

// inline implementations

static inline uint32_t get_offset_to_end_of_worm_any_page_header(const worm_tuple_defs* wtd_p)
{
	return get_offset_to_end_of_common_page_header(wtd_p->pas_p) + wtd_p->pas_p->page_id_width;
}

static inline uint64_t get_next_page_id_of_worm_any_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	return get_worm_any_page_header(ppage, wtd_p).next_page_id;
}

static inline uint32_t get_offset_to_worm_any_page_header_locals(const worm_tuple_defs* wtd_p)
{
	return get_offset_to_end_of_common_page_header(wtd_p->pas_p);
}

static inline worm_any_page_header get_worm_any_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	const void* worm_any_page_header_serial = get_page_header_ua_persistent_page(ppage, wtd_p->pas_p->page_size) + get_offset_to_worm_any_page_header_locals(wtd_p);
	return (worm_any_page_header){
		.parent = get_common_page_header(ppage, wtd_p->pas_p),
		.next_page_id = deserialize_uint64(worm_any_page_header_serial, wtd_p->pas_p->page_id_width),
	};
}

static inline void serialize_worm_any_page_header(void* hdr_serial, const worm_any_page_header* waph_p, const worm_tuple_defs* wtd_p)
{
	serialize_common_page_header(hdr_serial, &(waph_p->parent), wtd_p->pas_p);

	void* worm_any_page_header_serial = hdr_serial + get_offset_to_worm_any_page_header_locals(wtd_p);
	serialize_uint64(worm_any_page_header_serial, wtd_p->pas_p->page_id_width, waph_p->next_page_id);
}

static inline void set_worm_any_page_header(persistent_page* ppage, const worm_any_page_header* waph_p, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, wtd_p->pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, wtd_p->pas_p->page_size), page_header_size);

	// serialize waph_p on the hdr_serial
	serialize_worm_any_page_header(hdr_serial, waph_p, wtd_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, wtd_p->pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

static inline void print_worm_any_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	print_common_page_header(ppage, wtd_p->pas_p);
	printf("next_page_id : %"PRIu64"\n", get_next_page_id_of_worm_any_page(ppage, wtd_p));
}

#endif