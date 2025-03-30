#ifndef BPLUS_TREE_LEAF_PAGE_HEADER_H
#define BPLUS_TREE_LEAF_PAGE_HEADER_H

#include<tupleindexer/common/common_page_header.h>
#include<tupleindexer/bplus_tree/bplus_tree_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>
#include<tupleindexer/utils/persistent_page_functions.h>

#include<serint/serial_int.h>

#include<stdlib.h>

typedef struct bplus_tree_leaf_page_header bplus_tree_leaf_page_header;
struct bplus_tree_leaf_page_header
{
	common_page_header parent;

	// all leaf pages are doubly linked, with pointers (page_id-s) to next and prev pages
	uint64_t next_page_id;

	uint64_t prev_page_id;
};

#define sizeof_BPLUS_TREE_LEAF_PAGE_HEADER get_offset_to_end_of_bplus_tree_leaf_page_header

static inline uint32_t get_offset_to_end_of_bplus_tree_leaf_page_header(const bplus_tree_tuple_defs* bpttd_p);

static inline uint64_t get_next_page_id_of_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

static inline uint64_t get_prev_page_id_of_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

static inline bplus_tree_leaf_page_header get_bplus_tree_leaf_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

static inline void serialize_bplus_tree_leaf_page_header(void* hdr_serial, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p);

static inline void set_bplus_tree_leaf_page_header(persistent_page* ppage, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// prints header of bplus_tree leaf page
static inline void print_bplus_tree_leaf_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

// inline implementations

static inline uint32_t get_offset_to_end_of_bplus_tree_leaf_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_common_page_header(bpttd_p->pas_p) + (2 * bpttd_p->pas_p->page_id_width);
}

static inline uint64_t get_next_page_id_of_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_leaf_page_header(ppage, bpttd_p).next_page_id;
}

static inline uint64_t get_prev_page_id_of_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_leaf_page_header(ppage, bpttd_p).prev_page_id;
}

static inline uint32_t get_offset_to_bplus_tree_leaf_page_header_locals(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_common_page_header(bpttd_p->pas_p);
}

static inline bplus_tree_leaf_page_header get_bplus_tree_leaf_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* leaf_page_header_serial = get_page_header_ua_persistent_page(ppage, bpttd_p->pas_p->page_size) + get_offset_to_bplus_tree_leaf_page_header_locals(bpttd_p);
	return (bplus_tree_leaf_page_header){
		.parent = get_common_page_header(ppage, bpttd_p->pas_p),
		.next_page_id = deserialize_uint64(leaf_page_header_serial, bpttd_p->pas_p->page_id_width),
		.prev_page_id = deserialize_uint64(leaf_page_header_serial + bpttd_p->pas_p->page_id_width, bpttd_p->pas_p->page_id_width),
	};
}

static inline void serialize_bplus_tree_leaf_page_header(void* hdr_serial, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p)
{
	serialize_common_page_header(hdr_serial, &(bptlph_p->parent), bpttd_p->pas_p);

	void* bplus_tree_leaf_page_header_serial = hdr_serial + get_offset_to_bplus_tree_leaf_page_header_locals(bpttd_p);
	serialize_uint64(bplus_tree_leaf_page_header_serial, bpttd_p->pas_p->page_id_width, bptlph_p->next_page_id);
	serialize_uint64(bplus_tree_leaf_page_header_serial + bpttd_p->pas_p->page_id_width, bpttd_p->pas_p->page_id_width, bptlph_p->prev_page_id);
}

static inline void set_bplus_tree_leaf_page_header(persistent_page* ppage, const bplus_tree_leaf_page_header* bptlph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, bpttd_p->pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, bpttd_p->pas_p->page_size), page_header_size);

	// serialize bptlph_p on the hdr_serial
	serialize_bplus_tree_leaf_page_header(hdr_serial, bptlph_p, bpttd_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, bpttd_p->pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

static inline void print_bplus_tree_leaf_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	print_common_page_header(ppage, bpttd_p->pas_p);
	printf("next_page_id : %"PRIu64"\n", get_next_page_id_of_bplus_tree_leaf_page(ppage, bpttd_p));
	printf("prev_page_id : %"PRIu64"\n", get_prev_page_id_of_bplus_tree_leaf_page(ppage, bpttd_p));
}

#endif