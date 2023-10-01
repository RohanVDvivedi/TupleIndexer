#ifndef DATA_ACCESS_METHODS_H
#define DATA_ACCESS_METHODS_H

#include<stdint.h>

/*
**	This structure defines functions that provide access methods to the storage model to access database
*/

/*
**	All operations must return 1 or Non NULL when they have succeeded
**  0 or NULL impiles an error with the corresponding operation
*/

// below are the options that can go with the functions below
#define NO_OPTION    0b000
#define FREE_PAGE    0b001
#define FORCE_FLUSH  0b010
#define WAS_MODIFIED 0b100

typedef struct data_access_methods data_access_methods;
struct data_access_methods
{
	int (*open_data_file)(void* context);

	// a request method to get a new blank page from the page manager with write lock on the page
	// the page_id_returned is set with the page_id of the new_page
	void* (*get_new_page_with_write_lock)(void* context, uint64_t* page_id_returned);

	// locks a page for read or write, if successfull must return pointer to the in-memory of the page
	void* (*acquire_page_with_reader_lock)(void* context, uint64_t page_id);
	void* (*acquire_page_with_writer_lock)(void* context, uint64_t page_id);

	// downgrade a writer lock to a reader lock
	int (*downgrade_writer_lock_to_reader_lock_on_page)(void* context, void* pg_ptr, int opts); // acceptable options : WAS_MODIFIED and FORCE_FLUSH
	int (*upgrade_reader_lock_to_writer_lock_on_page)(void* context, void* pg_ptr);

	// releases lock on the page, accordingly, free_page flag will free the page, after releasing the lock
	int (*release_reader_lock_on_page)(void* context, void* pg_ptr, int opts); // acceptable options : FREE_PAGE
	int (*release_writer_lock_on_page)(void* context, void* pg_ptr, int opts); // acceptable options : WAS_MODIFIED, FORCE_FLUSH and FREE_PAGE

	// the was_modified parameter suggests that the page that we had a writer lock on was modified,
	// and any disk based system is suppossed to mark this page dirty now and possibly persist this new version of page to disk,
	// whenever it deems necessary OR try to flush it immediately, if the force_flush param is set

	// make a page free, you may call this function, even if you don't have lock on the page
	// fails only if the page is already free
	int (*free_page)(void* context, uint64_t page_id);

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