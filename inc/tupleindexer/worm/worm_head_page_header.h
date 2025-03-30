#ifndef WORM_HEAD_PAGE_HEADER_H
#define WORM_HEAD_PAGE_HEADER_H

#include<tupleindexer/common/common_page_header.h>
#include<tupleindexer/worm/worm_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>
#include<tupleindexer/utils/persistent_page_functions.h>

#include<serint/serial_int.h>

#include<stdlib.h>

typedef struct worm_head_page_header worm_head_page_header;
struct worm_head_page_header
{
	common_page_header parent;

	uint64_t reference_counter; // the worm is deleted if the reference_counter reaches 0, it always must start with 1

	// reference_counter is always sized to uint64_t
	// all the other below attributes are page_ids, so are pas_p->page_id_width bytes wide

	uint64_t dependent_root_page_id; // dependent_root data structure that also gets deleted if the reference_counter reaches 0

	uint64_t tail_page_id; // tail of this worm, could be head page_id

	// all pages including the head are singly linked, with pointers (page_id-s) to next page
	uint64_t next_page_id; // could be NULL_PAGE_ID if it is the tail
};

#define sizeof_WORM_HEAD_PAGE_HEADER get_offset_to_end_of_worm_head_page_header

static inline uint32_t get_offset_to_end_of_worm_head_page_header(const worm_tuple_defs* wtd_p);

static inline uint64_t get_tail_page_id_of_worm_head_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

static inline uint64_t get_next_page_id_of_worm_head_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

static inline worm_head_page_header get_worm_head_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

static inline void serialize_worm_head_page_header(void* hdr_serial, const worm_head_page_header* whph_p, const worm_tuple_defs* wtd_p);

static inline void set_worm_head_page_header(persistent_page* ppage, const worm_head_page_header* whph_p, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// prints header of worm head page
static inline void print_worm_head_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

// inline implementations

static inline uint32_t get_offset_to_end_of_worm_head_page_header(const worm_tuple_defs* wtd_p)
{
	return get_offset_to_end_of_common_page_header(wtd_p->pas_p) + sizeof(uint64_t) + (3 * wtd_p->pas_p->page_id_width);
}

static inline uint64_t get_tail_page_id_of_worm_head_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	return get_worm_head_page_header(ppage, wtd_p).tail_page_id;
}

static inline uint64_t get_next_page_id_of_worm_head_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	return get_worm_head_page_header(ppage, wtd_p).next_page_id;
}

static inline uint32_t get_offset_to_worm_head_page_header_locals(const worm_tuple_defs* wtd_p)
{
	return get_offset_to_end_of_common_page_header(wtd_p->pas_p);
}

static inline worm_head_page_header get_worm_head_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	const void* worm_head_page_header_serial = get_page_header_ua_persistent_page(ppage, wtd_p->pas_p->page_size) + get_offset_to_worm_head_page_header_locals(wtd_p);
	return (worm_head_page_header){
		.parent = get_common_page_header(ppage, wtd_p->pas_p),
		.reference_counter =      deserialize_uint64(worm_head_page_header_serial, sizeof(uint64_t)),
		.dependent_root_page_id = deserialize_uint64(worm_head_page_header_serial + sizeof(uint64_t), wtd_p->pas_p->page_id_width),
		.tail_page_id =           deserialize_uint64(worm_head_page_header_serial + sizeof(uint64_t) + wtd_p->pas_p->page_id_width, wtd_p->pas_p->page_id_width),
		.next_page_id =           deserialize_uint64(worm_head_page_header_serial + sizeof(uint64_t) + 2 * wtd_p->pas_p->page_id_width, wtd_p->pas_p->page_id_width),
	};
}

static inline void serialize_worm_head_page_header(void* hdr_serial, const worm_head_page_header* whph_p, const worm_tuple_defs* wtd_p)
{
	serialize_common_page_header(hdr_serial, &(whph_p->parent), wtd_p->pas_p);

	void* worm_head_page_header_serial = hdr_serial + get_offset_to_worm_head_page_header_locals(wtd_p);
	serialize_uint64(worm_head_page_header_serial, sizeof(uint64_t), whph_p->reference_counter);
	serialize_uint64(worm_head_page_header_serial + sizeof(uint64_t), wtd_p->pas_p->page_id_width, whph_p->dependent_root_page_id);
	serialize_uint64(worm_head_page_header_serial + sizeof(uint64_t) + wtd_p->pas_p->page_id_width, wtd_p->pas_p->page_id_width, whph_p->tail_page_id);
	serialize_uint64(worm_head_page_header_serial + sizeof(uint64_t) + 2 * wtd_p->pas_p->page_id_width, wtd_p->pas_p->page_id_width, whph_p->next_page_id);
}

static inline void set_worm_head_page_header(persistent_page* ppage, const worm_head_page_header* whph_p, const worm_tuple_defs* wtd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, wtd_p->pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, wtd_p->pas_p->page_size), page_header_size);

	// serialize whph_p on the hdr_serial
	serialize_worm_head_page_header(hdr_serial, whph_p, wtd_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, wtd_p->pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

static inline void print_worm_head_page_header(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	print_common_page_header(ppage, wtd_p->pas_p);
	const worm_head_page_header whph = get_worm_head_page_header(ppage, wtd_p);
	printf("reference_counter : %"PRIu64"\n", whph.reference_counter);
	printf("dependent_root_page_id : %"PRIu64"\n", whph.dependent_root_page_id);
	printf("tail_page_id : %"PRIu64"\n", whph.tail_page_id);
	printf("next_page_id : %"PRIu64"\n", whph.next_page_id);
}

#endif