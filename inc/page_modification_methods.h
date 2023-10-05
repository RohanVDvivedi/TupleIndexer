#ifndef PAGE_MODIFICATION_METHODS_H
#define PAGE_MODIFICATION_METHODS_H

#include<stdint.h>

#include<tuple_def.h>
#include<persistent_page.h>

/*
** This functions act as a middleware allowing you to intercept the functions that modify the page using TupleStore's methods
** It allows you to Write Ahead Log the operations while modifying the page
** Note :: They must be backed by the corresponding functions from TupleStore ONLY
*/

// All the functions of page_modification_methods are analogous to their corresponding counterparts in page_layout.h

typedef struct page_modification_methods page_modification_methods;
struct page_modification_methods
{
	int (*init_page)(void* context, persistent_page ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d);

	void (*set_page_header)(void* context, persistent_page ppage, uint32_t page_size, const void* hdr);

	int (*append_tuple_on_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple);

	int (*update_tuple_on_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

	int (*discard_tuple_on_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index);

	void (*discard_all_tuples_on_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

	uint32_t (*discard_trailing_tomb_stones_on_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

	int (*swap_tuples_on_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2);

	int (*set_element_in_tuple_in_place_on_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, uint32_t element_index, const user_value* value);

	void (*clone_page)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page ppage_src);

	// the below function, does not change contents of the page, hence a physiologic log entry for this operation is not required in most settings
	int (*run_page_compaction)(void* context, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

	void* context;
};

// helper resilient methods, to be used to avoid calls to run_page_compaction

int append_tuple_on_page_resiliently(const page_modification_methods* pmm_p, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple);

int update_tuple_on_page_resiliently(const page_modification_methods* pmm_p, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

#endif