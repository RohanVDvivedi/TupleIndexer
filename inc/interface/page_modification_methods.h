#ifndef PAGE_MODIFICATION_METHODS_H
#define PAGE_MODIFICATION_METHODS_H

#include<stdint.h>

#include<tuple_def.h>
#include<persistent_page.h>

/*
**	This functions act as a middleware allowing you to intercept the functions that modify the page using TupleStore's methods
**	They should be used to Write Ahead Log the operations while modifying the page
**	NOTE :: They must be backed by the corresponding functions from TupleStore ONLY
**
**	All the functions here take in persistent_page by value, this is a design choice, so as I not allow the implementer of page_modification_methods to modify the persistent_page
**	You may or may not choose to WAL log the operation, its up to you, the persistent_page given to these functions will always be .is_write_locked = 1, this library will ensure that
**
**	All the functions of page_modification_methods are analogous to their corresponding counterparts in page_layout.h
**
**	These functions either modify the page OR they dont, It is never expected of them, to give an error other than abort_error
**	In case, when a transaction for some reason is not able to put a WAL log entry for the corresponding operation on the page, I want you to kill the application using exit().
**	A database is not a database at all without its WAL, even if you fail to allocate memory to create a WAL record, I want you to exit()
**	These methods have the sole purpose to intercept the page modification and WAL (Write Ahead Log) it.
**
**	A WAL record should not be logged, if a transaction is found to be already aborted, in such a case you may return with an abort_error set to the reason of the abort
**	An abort error can not be 1. failure to allocate memory, OR 2. failure to WAL log -> in those cases you must exit()
**
**	Your design may include a shared/exclusive lock on the transaction's is_aborted bit and its reason for abort,
**	allowing multiple threads to get shared lock to ensure that the transaction is not aborted, while they write a log record for one of its changes
**	And take an exclusive lock to mark it aborted, which these functions may never do
**
**	Such a design ensures that no WAL records are inserted into WAL, after a transaction is aborted or commited
**
**	As discussed above, the following functions may just ensure that the transaction is not aborted, AND never mark it aborted
*/

typedef struct page_modification_methods page_modification_methods;
struct page_modification_methods
{
	int (*init_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d, int* abort_error);

	void (*set_page_header)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const void* hdr, int* abort_error);

	int (*append_tuple_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error);

	int (*insert_tuple_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error);

	int (*update_tuple_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error);

	int (*discard_tuple_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, int* abort_error);

	void (*discard_all_tuples_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error);

	uint32_t (*discard_trailing_tomb_stones_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error);

	int (*swap_tuples_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2, int* abort_error);

	int (*set_element_in_tuple_in_place_on_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, uint32_t element_index, const user_value* value, int* abort_error);

	void (*clone_page)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page ppage_src, int* abort_error);

	// the below function, does not change contents of the page, hence a physiologic log entry for this operation is not required in most settings
	// as this function never logs to WAL (That is what I expect), you may think about not throwing abort_error in this function
	int (*run_page_compaction)(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error);

	void* context;
};

#endif