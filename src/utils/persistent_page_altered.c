#include<persistent_page_altered.h>

#include<page_modification_methods.h>

#include<stdlib.h>

int init_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	int res = pmm_p->init_page(pmm_p->context, transaction_id, *ppage, page_size, page_header_size, tpl_sz_d, abort_error);

	// if the page was inited, then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}

void set_persistent_page_header(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const void* hdr, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	pmm_p->set_page_header(pmm_p->context, transaction_id, *ppage, page_size, hdr, abort_error);

	// always assumed to be modified
	if(!(*(abort_error)))
		ppage->flags |= WAS_MODIFIED;
}

int append_tuple_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	int res = pmm_p->append_tuple_on_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, external_tuple, abort_error);

	// if the page was updated, then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}

int update_tuple_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	int res = pmm_p->update_tuple_on_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, index, external_tuple, abort_error);

	// if the page was updated, then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}

int discard_tuple_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	int res = pmm_p->discard_tuple_on_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, index, abort_error);

	// if the page was updated, then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}

void discard_all_tuples_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	pmm_p->discard_all_tuples_on_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, abort_error);

	// always assumed to be modified
	if(!(*abort_error))
		ppage->flags |= WAS_MODIFIED;
}

uint32_t discard_trailing_tomb_stones_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	uint32_t res = pmm_p->discard_trailing_tomb_stones_on_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, abort_error);

	// if the page was updated (is atleast 1 tuple was discarded), then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}

int swap_tuples_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	// swapping a tuple with itself is a NO-OP
	// the i1 and i2 are expected to be within bounds of the tuple_count on the page
	if(i1 == i2)
		return 1;

	int res = pmm_p->swap_tuples_on_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, i1, i2, abort_error);

	// if the page was updated, then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}

int set_element_in_tuple_in_place_on_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, uint32_t element_index, const user_value* value, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	int res = pmm_p->set_element_in_tuple_in_place_on_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_d, tuple_index, element_index, value, abort_error);

	// if the page was updated, then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}

void clone_persistent_page(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page* ppage_src, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	pmm_p->clone_page(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, *ppage_src, abort_error);

	// always assumed to be modified
	if(!(*abort_error))
		ppage->flags |= WAS_MODIFIED;
}

int run_persistent_page_compaction(const page_modification_methods* pmm_p, const void* transaction_id, persistent_page* ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	// if a persistent_page is not write locked, then you can not write to it
	if(!is_persistent_page_write_locked(ppage))
	{
		printf("BUG :: attempting to write to read locked page\n");
		exit(-1);
	}

	// attempt to modify a page after an abort
	if(*(abort_error))
	{
		printf("BUG :: attempting to modify a page, after knowing of an abort\n");
		exit(-1);
	}

	int res = pmm_p->run_page_compaction(pmm_p->context, transaction_id, *ppage, page_size, tpl_sz_d, abort_error);

	// if the page was compacted, then set the WAS_MODIFIED bit of the ppage flag
	if(res && (!(*abort_error)))
		ppage->flags |= WAS_MODIFIED;

	// if aborted modification is not expected to have been performed
	if((*abort_error))
		return 0;

	return res;
}