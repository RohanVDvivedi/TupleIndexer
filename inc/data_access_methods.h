#ifndef DATA_ACCESS_METHODS_H
#define DATA_ACCESS_METHODS_H

#include<stdint.h>

/*
**	This structure defines functions that provide access methods to the storage model to access database
*/

/*
**	All operations must return 1 or Non NULL when they have succeeded
*/

typedef struct data_access_methods data_access_methods;
struct data_access_methods
{
	int (*open_data_file)(const void* context);

	// a request method to get a new blank page from the page manager with write lock on the page
	// the page_id_returned is set with the page_id of the new_page
	void* (*get_blank_page_with_write_lock)(const void* context, uint32_t* page_id_returned);

	// locks a page for read or write, if successfull must return pointer to the in-memory of the page
	void* (*acquire_page_with_reader_lock)(const void* context, uint32_t page_id);
	void* (*acquire_page_with_writer_lock)(const void* context, uint32_t page_id);
	void* (*downgrade_writer_to_reader_lock)(const void* context, void* pg_ptr, uint32_t page_id);

	// releases lock on the page, accordingly
	int (*release_reader_lock)(const void* context, void* pg_ptr);
	int (*release_writer_lock)(const void* context, void* pg_ptr);

	// equivalent to msync
	// you must call this function while holding a reader lock on the page
	int (*force_write_to_disk)(const void* context, uint32_t page_id);

	int (*close_data_file)(const void* context);

	uint32_t page_size_in_bytes;

	// context to pass on every page access
	const void* context;
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
**	  N -> R 		by calling acquire_reader_lock
**	  R -> W 		by calling upgrade_reader_to_writer_lock
**	  W -> N 		by calling release_writer_lock
**
*/

#endif