#ifndef BPLUS_TREE_INTERIOR_PAGE_HEADER_H
#define BPLUS_TREE_INTERIOR_PAGE_HEADER_H

#include<tupleindexer/common/common_page_header.h>
#include<tupleindexer/bplus_tree/bplus_tree_tuple_definitions.h>
#include<tupleindexer/interface/opaque_page_modification_methods.h>
#include<tupleindexer/utils/persistent_page_functions.h>

#include<serint/serial_int.h>

#include<stdlib.h>

typedef struct bplus_tree_interior_page_header bplus_tree_interior_page_header;
struct bplus_tree_interior_page_header
{
	common_page_header parent;

	// level of the page in bplus_tree,
	// always > 0 interior page
	// 1 -> interior page level right above the leaf page
	uint32_t level;

	// all leaf pages are doubly linked, with pointers (page_id-s) to next and prev pages
	uint64_t least_keys_page_id;

	// flag that suggests if this interior page is last one on this bplus_tree level
	int is_last_page_of_level;
};

// number of bytes in the flags field of the header
#define FLAGS_BYTE_SIZE 1

// bit position of is_last_page_of_level
#define IS_LAST_PAGE_OF_LEVEL_FLAG_POS 0

// number of bytes to store level of the page
// 2 is more than enough for curent computer systems, and allows us to store large amount of data
// values in range 1 to 4 both inclusive
#define BYTES_FOR_PAGE_LEVEL 2

#define sizeof_BPLUS_TREE_INTERIOR_PAGE_HEADER get_offset_to_end_of_bplus_tree_interior_page_header

static inline uint32_t get_offset_to_end_of_bplus_tree_interior_page_header(const bplus_tree_tuple_defs* bpttd_p);

static inline uint32_t get_level_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

static inline uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

static inline int is_last_page_of_level_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

static inline bplus_tree_interior_page_header get_bplus_tree_interior_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

static inline void serialize_bplus_tree_interior_page_header(void* hdr_serial, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p);

static inline void set_bplus_tree_interior_page_header(persistent_page* ppage, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// prints header of bplus_tree interior page
static inline void print_bplus_tree_interior_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p);

// inline implementations

static inline uint32_t get_offset_to_end_of_bplus_tree_interior_page_header(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_common_page_header(bpttd_p->pas_p) + BYTES_FOR_PAGE_LEVEL + bpttd_p->pas_p->page_id_width + FLAGS_BYTE_SIZE;
}

static inline uint32_t get_level_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_interior_page_header(ppage, bpttd_p).level;
}

static inline uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_interior_page_header(ppage, bpttd_p).least_keys_page_id;
}

static inline int is_last_page_of_level_of_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	return get_bplus_tree_interior_page_header(ppage, bpttd_p).is_last_page_of_level;
}

static inline uint32_t get_offset_to_bplus_tree_interior_page_header_locals(const bplus_tree_tuple_defs* bpttd_p)
{
	return get_offset_to_end_of_common_page_header(bpttd_p->pas_p);
}

static inline bplus_tree_interior_page_header get_bplus_tree_interior_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	const void* interior_page_header_serial = get_page_header_ua_persistent_page(ppage, bpttd_p->pas_p->page_size) + get_offset_to_bplus_tree_interior_page_header_locals(bpttd_p);
	return (bplus_tree_interior_page_header){
		.parent = get_common_page_header(ppage, bpttd_p->pas_p),
		.level = deserialize_uint32(interior_page_header_serial, BYTES_FOR_PAGE_LEVEL),
		.least_keys_page_id = deserialize_uint64(interior_page_header_serial + BYTES_FOR_PAGE_LEVEL, bpttd_p->pas_p->page_id_width),
		.is_last_page_of_level = ((deserialize_int8(interior_page_header_serial + BYTES_FOR_PAGE_LEVEL + bpttd_p->pas_p->page_id_width, FLAGS_BYTE_SIZE) >> IS_LAST_PAGE_OF_LEVEL_FLAG_POS) & 1),
	};
}

static inline void serialize_bplus_tree_interior_page_header(void* hdr_serial, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p)
{
	serialize_common_page_header(hdr_serial, &(bptiph_p->parent), bpttd_p->pas_p);

	void* bplus_tree_interior_page_header_serial = hdr_serial + get_offset_to_bplus_tree_interior_page_header_locals(bpttd_p);
	serialize_uint32(bplus_tree_interior_page_header_serial, BYTES_FOR_PAGE_LEVEL, bptiph_p->level);
	serialize_uint64(bplus_tree_interior_page_header_serial + BYTES_FOR_PAGE_LEVEL, bpttd_p->pas_p->page_id_width, bptiph_p->least_keys_page_id);
	serialize_uint64(bplus_tree_interior_page_header_serial + BYTES_FOR_PAGE_LEVEL + bpttd_p->pas_p->page_id_width, FLAGS_BYTE_SIZE, ((!!(bptiph_p->is_last_page_of_level)) << IS_LAST_PAGE_OF_LEVEL_FLAG_POS));
}

static inline void set_bplus_tree_interior_page_header(persistent_page* ppage, const bplus_tree_interior_page_header* bptiph_p, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	uint32_t page_header_size = get_page_header_size_persistent_page(ppage, bpttd_p->pas_p->page_size);

	// allocate memory, to hold complete page_header
	void* hdr_serial = malloc(page_header_size);
	if(hdr_serial == NULL)
		exit(-1);

	// copy the old page_header to it
	memory_move(hdr_serial, get_page_header_ua_persistent_page(ppage, bpttd_p->pas_p->page_size), page_header_size);

	// serialize bptlph_p on the hdr_serial
	serialize_bplus_tree_interior_page_header(hdr_serial, bptiph_p, bpttd_p);

	// write hdr_serial to the new header position
	set_persistent_page_header(pmm_p, transaction_id, ppage, bpttd_p->pas_p->page_size, hdr_serial, abort_error);

	// we need to free hdr_serial, even on an abort_error
	free(hdr_serial);
}

static inline void print_bplus_tree_interior_page_header(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	print_common_page_header(ppage, bpttd_p->pas_p);
	printf("level : %"PRIu32"\n", get_level_of_bplus_tree_interior_page(ppage, bpttd_p));
	printf("least_keys_page_id : %"PRIu64"\n", get_least_keys_page_id_of_bplus_tree_interior_page(ppage, bpttd_p));
	printf("is_last_page_of_level : %d\n", is_last_page_of_level_of_bplus_tree_interior_page(ppage, bpttd_p));
}

#endif