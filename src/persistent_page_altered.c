#include<persistent_page_altered.h>

#include<page_modification_methods.h>
#include<data_access_methods.h>

#include<stdlib.h>

int init_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("attempting to write to read locked page");
		exit(-1);
	}

	int res = pmm_p->init_page(pmm_p->context, *ppage, page_size, page_header_size, tpl_sz_d);

	// if the page was inited, then set the WAS_MODIFIED bit of the ppage flag
	if(res)
		ppage->flags |= WAS_MODIFIED;

	return res;
}

void set_persistent_page_header(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const void* hdr)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("attempting to write to read locked page");
		exit(-1);
	}

	pmm_p->set_page_header(pmm_p->context, *ppage, page_size, hdr);

	ppage->flags |= WAS_MODIFIED;
}

int append_tuple_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("attempting to write to read locked page");
		exit(-1);
	}

	int res = pmm_p->append_tuple_on_page(pmm_p->context, *ppage, page_size, tpl_sz_d, external_tuple);

	// if the page was inited, then set the WAS_MODIFIED bit of the ppage flag
	if(res)
		ppage->flags |= WAS_MODIFIED;

	return res;
}

int update_tuple_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("attempting to write to read locked page");
		exit(-1);
	}

	int res = pmm_p->update_tuple_on_page(pmm_p->context, *ppage, page_size, tpl_sz_d, index, external_tuple);

	// if the page was inited, then set the WAS_MODIFIED bit of the ppage flag
	if(res)
		ppage->flags |= WAS_MODIFIED;

	return res;
}

int discard_tuple_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index);

void discard_all_tuples_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

uint32_t discard_trailing_tomb_stones_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);

int swap_tuples_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2);

int set_element_in_tuple_in_place_on_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, uint32_t element_index, const user_value* value);

void clone_persistent_page(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page* ppage_src);

int run_persistent_page_compaction(const page_modification_methods* pmm_p, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d);