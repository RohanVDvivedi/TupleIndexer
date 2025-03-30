#ifndef WORM_PAGE_HEADER_H
#define WORM_PAGE_HEADER_H

#include<tupleindexer/utils/persistent_page.h>
#include<tupleindexer/worm/worm_tuple_definitions.h>

#include<tupleindexer/common/common_page_header.h>
#include<tupleindexer/worm/worm_head_page_header.h>
#include<tupleindexer/worm/worm_any_page_header.h>


/*
**	utility functions for worm pages
*/

static inline int is_worm_head_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);
static inline int is_worm_tail_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

static inline uint64_t get_next_page_id_of_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p);

// inline implementations

static inline int is_worm_head_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	return get_common_page_header(ppage, wtd_p->pas_p).type == WORM_HEAD_PAGE;
}

static inline uint64_t get_next_page_id_of_worm_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	if(is_worm_head_page(ppage, wtd_p))
		return get_next_page_id_of_worm_head_page(ppage, wtd_p);
	else
		return get_next_page_id_of_worm_any_page(ppage, wtd_p);
}

static inline int is_worm_tail_page(const persistent_page* ppage, const worm_tuple_defs* wtd_p)
{
	return get_next_page_id_of_worm_page(ppage, wtd_p) == wtd_p->pas_p->NULL_PAGE_ID;
}

#endif