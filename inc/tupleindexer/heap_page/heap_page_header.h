#ifndef ARRAY_TABLE_PAGE_HEADER_H
#define ARRAY_TABLE_PAGE_HEADER_H

#include<tupleindexer/common/common_page_header.h>
#include<tupleindexer/common/page_access_specification.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>
#include<tupleindexer/utils/persistent_page_functions.h>

#include<serint/serial_int.h>

#include<stdlib.h>

typedef struct heap_page_header heap_page_header;
struct heap_page_header
{
	common_page_header parent;

	// heap_page_header has nothing except the type of the page
};

#define sizeof_HEAP_PAGE_HEADER get_offset_to_end_of_heap_page_header

static inline uint32_t get_offset_to_end_of_heap_page_header(const page_access_specs* pas_p);

static inline heap_page_header get_heap_page_header(const persistent_page* ppage, const page_access_specs* pas_p);

static inline void serialize_heap_page_header(void* hdr_serial, const heap_page_header* hph_p, const page_access_specs* pas_p);

static inline void set_heap_page_header(persistent_page* ppage, const heap_page_header* hph_p, const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

static inline void print_heap_page_header(const persistent_page* ppage, const page_access_specs* pas_p);

// inline implementations

static inline uint32_t get_offset_to_end_of_heap_page_header(const page_access_specs* pas_p)
{
	return get_offset_to_end_of_common_page_header(pas_p); // + nothing because it has nothing except the type of the page
}

static inline uint32_t get_offset_to_heap_page_header_locals(const page_access_specs* pas_p)
{
	return get_offset_to_end_of_common_page_header(pas_p);
}

static inline heap_page_header get_heap_page_header(const persistent_page* ppage, const page_access_specs* pas_p)
{
	//const void* heap_page_header_serial = get_page_header_ua_persistent_page(ppage, pas_p->page_size) + get_offset_to_heap_page_header_locals(pas_p);
	return (heap_page_header){
		.parent = get_common_page_header(ppage, pas_p),
	};
}

static inline void serialize_heap_page_header(void* hdr_serial, const heap_page_header* hph_p, const page_access_specs* pas_p)
{
	serialize_common_page_header(hdr_serial, &(hph_p->parent), pas_p);

	//void* heap_page_header_serial = hdr_serial + get_offset_to_heap_page_header_locals(pas_p);
}

static inline void set_heap_page_header(persistent_page* ppage, const heap_page_header* hph_p, const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, pas_p->page_size), page_header_size);

	// serialize hph_p on the hdr_serial
	serialize_heap_page_header(hdr_serial, hph_p, pas_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

static inline void print_heap_page_header(const persistent_page* ppage, const page_access_specs* pas_p)
{
	print_common_page_header(ppage, pas_p);
}

#endif