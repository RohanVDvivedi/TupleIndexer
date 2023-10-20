#ifndef DATA_ACCESS_METHODS_H
#define DATA_ACCESS_METHODS_H

#include<stdint.h>

/*
**	This structure defines functions that provide page level access methods to the storage model to access the database in pages of fixed size
*/

/*
**	All operations must return 1 or Non NULL when they have succeeded
**  0 or NULL impiles an abort_error/a failure with the corresponding operation
**	a failure to data_access_methods function calls must accompany with an abort_error
**	this the same error that transaction abort manager will be marked aborted with
**	any error returned, is expected to abort the transaction, and must be returned with abort_error
**	In case of a failure to allocate more memeory for your needs, do not throw an abort_error, just call exit() and kill the application
**	Additionally ensure that no new page locks are granted once a transaction is aborted
*/

/*
**	Your design may include a shared lock on the transaction's is_aborted bit and its reason for abort, allowing multiple threads to ensure that the transaction is not aborted while you write a log record for one of its changes
**	And take an exclusive lock to mark it aborted
**
**	As discussed above, the following functions may just ensure that the transaction is not aborted, AND mark it aborted if the corresponding function fails
**
**	In situation, when you are handed an aborted transaction, to get some operation done with the below functions
**	All you need to do is set the abort_error with the reason of abort and return
*/

// below are the options that can go with the functions below
//#define NONE_OPTION  0b000
//#define FREE_PAGE    0b001
//#define FORCE_FLUSH  0b010
//#define WAS_MODIFIED 0b100
// moved to data_access_methods_options.h
#include<data_access_methods_options.h>

typedef struct data_access_methods data_access_methods;
struct data_access_methods
{
	int (*open_data_file)(void* context);

	// a request method to get a new blank page from the page manager with write lock on the page
	// the page_id_returned is set with the page_id of the new_page
	void* (*get_new_page_with_write_lock)(void* context, const void* transaction_id, uint64_t* page_id_returned, int* abort_error);

	// locks a page for read or write, if successfull must return pointer to the in-memory of the page
	void* (*acquire_page_with_reader_lock)(void* context, const void* transaction_id, uint64_t page_id, int* abort_error);
	void* (*acquire_page_with_writer_lock)(void* context, const void* transaction_id, uint64_t page_id, int* abort_error);

	// downgrade a writer lock to a reader lock
	int (*downgrade_writer_lock_to_reader_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error); // acceptable options : WAS_MODIFIED and FORCE_FLUSH
	int (*upgrade_reader_lock_to_writer_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int* abort_error);

	// releases lock on the page, accordingly, free_page flag will free the page, after releasing the lock
	int (*release_reader_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error); // acceptable options : FREE_PAGE
	int (*release_writer_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error); // acceptable options : WAS_MODIFIED, FORCE_FLUSH and FREE_PAGE

	// the was_modified parameter suggests that the page that we had a writer lock on was modified,
	// and any disk based system is suppossed to mark this page dirty now and possibly persist this new version of page to disk,
	// whenever it deems necessary OR try to flush it immediately, if the force_flush param is set

	// make a page free, you may call this function, even if you don't have lock on the page
	// fails only if the page is already free
	int (*free_page)(void* context, const void* transaction_id, uint64_t page_id, int* abort_error);

	int (*close_data_file)(void* context);

	// size of page in bytes
	uint32_t page_size;

	// number of bytes in the page id
	// this can be 1, 2, 4, or 8
	uint8_t page_id_width;

	// a page with this page_id number should/would never be allocated or used by the system
	// this page_id implies something similar to a NULL pointer
	// NULL_PAGE_ID < (1 << (page_id_width * 8))
	uint64_t NULL_PAGE_ID;

	// context to be passed on every page access
	void* context;
};

// Lock transitions allowed for any page in the data store
/*
**
**	No lock     => N
**	Reader lock => R
**	Writer lock => W
**
**	Lock transitions allowed
**
**	  N -> R 		by calling acquire_page_with_reader_lock
**	  R -> N 		by calling release_reader_lock_on_page
**	  N -> W 		by calling acquire_page_with_writer_lock
**	  W -> N 		by calling release_writer_lock_on_page
**	  W -> R 		by calling downgrade_writer_lock_to_reader_lock_on_page
**	  R -> W 		by calling upgrade_reader_lock_to_writer_lock_on_page
**
*/

#endif