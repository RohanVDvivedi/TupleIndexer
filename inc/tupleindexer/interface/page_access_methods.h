#ifndef PAGE_ACCESS_METHODS_H
#define PAGE_ACCESS_METHODS_H

#include<stdint.h>

#include<page_access_specification.h>

/*
**	This structure defines functions that provide page level access methods to the storage model to access the database in pages of fixed size
**
**	All operations must return 1 or Non NULL when they have succeeded
**  0 or NULL impiles an abort_error/a failure with the corresponding operation
**	a failure to page_access_methods function calls must accompany with a non-zero abort_error
**	this the same error that you should mark the transaction with in your transaction manager
**	Any failure occurred, is expected to have aborted the transaction, and its reason must be returned in abort_error
**
**	In case of a failure to allocate more memory for your needs, do not throw an abort_error, just call exit() and kill the application
**	Additionally ensure that no new page locks are granted, to any thread working on the transaction, once a transaction is aborted.
**	In such a case, when a transaction is already aborted, just return its reason for abort in the abort_error, that will suffice.
**
**	Your design may include a shared lock on the transaction's is_aborted bit and its reason for abort
**	allowing multiple threads to take this shared lock, to ensure that the transaction is not aborted while you work on fulfilling the request
**	And take an exclusive lock to mark it aborted
**
**	As discussed above, the following functions must ensure that the transaction is not aborted, AND mark it aborted if the corresponding function fails
**
**	In situation, when you are handed an aborted transaction, to get some operation done with the below functions
**	All you need to do is set the abort_error with the reason of abort and return, (except for release_*_lock_on_page functions -> explained later)
**
**	When you are throwing an abort error, from within of any of these functions, it is you who needs to mark it aborted, with the reason of abort, so that other threads can see it
**	!! Noone else is responsible to abort a transaction for you, you need to do it yourself !!
**
**	The below functions may be termed as acquire_lock and release_lock, but these are in-effect latches,
**	For instance when the page is first accessed, you may latch and lock it.
**	On any subsequent calls, you should only latch the page, and not lock it, I hope you get it.
**	latching a page is equivalent to pinning it in bufferpool (for read or write respectively), releasing a latch may put that page out of bufferpool's cache
**	but it should still remain locked for the entirity of the transaction. (Just as you may have learned it in 2-phase lock).
**	From concurrency point of view, locks to the same transaction must pass, but WRITE latches to same page from different threads, even for same transaction must wait
**	Because if you allow 2 threads of the same transaction to have write latches, then they may write at once to the same page and corrupt it
**
**	TupleIndexer will call page_access_methods only to instruct what it wants latched and for what (reading or writing) and upgrade and downgrade latches
**	or finally to release latches.
**
**	You need to handle locks on your own using these functions given below.
**
**	Once a transaction is known to be aborted by a thread, no new acquire_lock, free_page, downgrade_lock or upgrade_lock calls will be made by TupleIndexer.
**	Only release_lock calls will be made (without FREE_PAGE bit set in the opts), and abort_error will set to indicate that the TupleIndexer's thread is already aware of the transaction being aborted
*/

/*
**	conditions for usage of FREE_PAGE, and free_page
**	fail with an abort, if the user tries to acquire lock on a free page,
**	allow freeing of a page, with release lock function, only if there is only 1 thread (with same lock type as the release lock function) having reader or writer lock on the page.
**	free_page function must succeed only if no locks are acquired on the page.
**	in case when release_*_lock_on_page() function is passed with a FREE_PAGE flag, and if the operation stands aborted, then the latch/lock on the page is not to be released, until later when TupleIndexer calls to release_lock() without the option of FREE_PAGE
**	an abort on a release_*_lock_on_page(FREE_PAGE), is always followed by a call to release_*_lock_on_page() (called without FREE_PAGE option), so that your page_access_methods will release the acquired latch for an aborted transaction
*/

// ANY FAILURE, IS EXPECTED TO ALWAYS RESULT IN AN ABORT ERROR, i.e.  THE TRANSACTION IS MEANT TO BE ABORTED ON FAILURE FROM THE FOLLOWING FUNCTIONS

// below are the options that can go with the functions below
//#define NONE_OPTION  0b000	// dummy for setting no options
//#define FREE_PAGE    0b001	// will be used only while releasing the page lock
//#define WAS_MODIFIED 0b010	// this suggests that the page has been modified, allowing you bitwise OR it with your dirty bit
// moved to page_access_methods_options.h
#include<page_access_methods_options.h>

typedef struct page_access_methods page_access_methods;
struct page_access_methods
{
	// a request method to get a new blank page from the page manager with write lock on the page
	// the page_id_returned is set with the page_id of the new_page
	void* (*get_new_page_with_write_lock)(void* context, const void* transaction_id, uint64_t* page_id_returned, int* abort_error);

	// locks a page for read or write, if successfull must return pointer to the in-memory of the page
	void* (*acquire_page_with_reader_lock)(void* context, const void* transaction_id, uint64_t page_id, int* abort_error);
	void* (*acquire_page_with_writer_lock)(void* context, const void* transaction_id, uint64_t page_id, int* abort_error);

	// downgrade a writer lock to a reader lock
	int (*downgrade_writer_lock_to_reader_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error); // acceptable options : WAS_MODIFIED
	int (*upgrade_reader_lock_to_writer_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int* abort_error);

	// releases lock on the page, accordingly, free_page flag will free the page, after releasing the lock
	int (*release_reader_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error); // acceptable options : FREE_PAGE
	int (*release_writer_lock_on_page)(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error); // acceptable options : WAS_MODIFIED and FREE_PAGE

	// the was_modified parameter suggests that the page that we had a writer lock on was modified,
	// and any disk based system is suppossed to mark this page dirty now and possibly persist this new version of page to disk, whenever it deems necessary

	// make a page free, you may call this function, even if you don't have lock on the page
	// fails only if the page is already free
	int (*free_page)(void* context, const void* transaction_id, uint64_t page_id, int* abort_error);

	// page access specification for all the pages in the data store
	// even though it is not a constant, you must not modify it, unless while you are creating it
	// a constructor of any page_access_methods must take in a page_access_specs struct as a suggestion (even here only system_header size remains the same as the suggested one)
	// this attribute must be utilized for creating *_tuple_defs struct of required type
	page_access_specs pas;

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