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

// All the operation of page_modification_methods will return an int error code that must be ALWAYS BE ONE of the following
#define PMM_SUCCESS            1
#define PMM_FAIL               0
#define PMM_FAILED_TO_WAL_LOG -1 // fatal error, the transaction needs to abort on this error

typedef struct page_modification_methods page_modification_methods;
struct page_modification_methods
{
	int (*init_page)(void* context, persistent_page page, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d);

	int (*set_page_header)(void* context, persistent_page page, uint32_t page_size, const void* hdr);

	int (*append_tuple_on_page)(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple);

	int (*update_tuple_on_page)(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

	int (*discard_tuple_on_page)(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index);

	int (*discard_all_tuples_on_page)(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d);

	int (*discard_trailing_tomb_stones_on_page)(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t* tomb_stones_discarded);

	int (*swap_tuples_on_page)(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2);

	int (*set_element_in_tuple_in_place_on_page)(void* context, persistent_page page, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, uint32_t element_index, const user_value* value);

	int (*clone_page)(void* context, persistent_page page, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page page_src);

	void* context;
};

#endif