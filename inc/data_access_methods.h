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
	int (*downgrade_writer_lock_to_reader_lock_on_page)(void* context, void* pg_ptr);

	// releases lock on the page, accordingly
	int (*release_reader_lock_on_page)(void* context, void* pg_ptr);
	int (*release_writer_lock_on_page)(void* context, void* pg_ptr);

	// releases writer lock and mark the page as free
	int (*release_writer_lock_and_free_page)(void* context, void* pg_ptr);

	// get page id corresponding to a pg_ptr
	uint64_t (*get_page_id_for_page)(void* context, void* pg_ptr);

	// equivalent to msync
	// you must call this function while holding a reader lock on the page
	int (*force_write_to_disk)(void* context, uint64_t page_id);

	int (*close_data_file)(void* context);

	// size of page in bytes
	uint32_t page_size;

	// number of bytes in the page id
	// this can be 1, 2, 4, or 8
	int page_id_width;

	// context to pass on every page access
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
**  case 1 :
**	  N -> R 		by calling acquire_reader_lock
**	  R -> N 		by calling release_reader_lock
**
**  case 2 :
**	  N -> W 		by calling acquire_writer_lock
**	  W -> N 		by calling release_writer_lock
**
**  case 3 :
**	  N -> W 		by calling acquire_writer_lock
**	  W -> R 		by calling downgrade_writer_lock_to_reader_lock
**	  R -> N 		by calling release_reader_lock
**
*/

#endif