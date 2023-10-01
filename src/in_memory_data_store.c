#include<in_memory_data_store.h>

#include<hashmap.h>
#include<bst.h>
#include<linkedlist.h>

#include<stddef.h>
#include<stdlib.h>
#include<pthread.h>

typedef struct page_descriptor page_descriptor;
struct page_descriptor
{
	// remains constant through out the lifetime of the page_descriptor
	uint64_t page_id;

	// if free then the page is present in free_page_descs
	// and page_memory = NULL
	int is_free;

	// this will be NULL for a free page_desc
	void* page_memory;

	// reader wrier lock for the page
	rwlock page_lock;

	// below are the 2 embedded nodes used by page_id_map and page_memory_map

	llnode page_id_map_node;

	llnode page_memory_map_node;

	// embedded node for free_page_descs

	bstnode free_page_descs_node;
};

page_descriptor* get_new_page_descriptor(uint64_t page_id, pthread_mutex_t* global_lock_p)
{
	page_descriptor* page_desc = malloc(sizeof(page_descriptor));
	page_desc->page_id = page_id;
	page_desc->is_free = 1;	// because initially page_memory = NULL
	page_desc->is_marked_for_freeing = 0;
	page_desc->page_memory = NULL;
	initialize_rwlock(&(page_desc->page_lock), global_lock_p);
	initialize_llnode(&(page_desc->page_id_map_node));
	initialize_llnode(&(page_desc->page_memory_map_node));
	initialize_bstnode(&(page_desc->free_page_descs_node));
	return page_desc;
}

void delete_page_descriptor(page_descriptor* page_desc)
{
	deinitialize_rwlock(&(page_desc->page_lock));
	free(page_desc);
}

static int compare_page_descs_by_page_ids(const void* page_desc1, const void* page_desc2)
{
	if(((const page_descriptor*)(page_desc1))->page_id == ((const page_descriptor*)(page_desc2))->page_id)
		return 0;
	else if(((const page_descriptor*)(page_desc1))->page_id > ((const page_descriptor*)(page_desc2))->page_id)
		return 1;
	else
		return -1;
}

static int compare_page_descs_by_page_memories(const void* page_desc1, const void* page_desc2)
{
	if(((const page_descriptor*)(page_desc1))->page_memory == ((const page_descriptor*)(page_desc2))->page_memory)
		return 0;
	else if(((const page_descriptor*)(page_desc1))->page_memory > ((const page_descriptor*)(page_desc2))->page_memory)
		return 1;
	else
		return -1;
}

static cy_uint hash_on_page_id(const void* page_desc)
{
	return ((const page_descriptor*)(page_desc))->page_id;
}

static cy_uint hash_on_page_memory(const void* page_desc)
{
	return ((uintptr_t)(((const page_descriptor*)(page_desc))->page_memory));
}

static void* allocate_page(uint32_t page_size)
{
	return malloc(page_size);
}

static int deallocate_page(void* page)
{
	free(page);
	return 1;
}

typedef struct memory_store_context memory_store_context;
struct memory_store_context
{
	pthread_mutex_t global_lock;

	// constant
	uint32_t page_size;

	// maximum number of pages this system is allowed to have
	// constant
	uint64_t MAX_PAGE_COUNT;

	// bst of free_pages, ordered by their page_ids
	// this pages are free (they dont have their page_memory populated)
	// we always allocate new page from the least page_id from free_page_descs
	// and release memory of greatest page_descs to OS, if it is equal to the total_pages_count - 1
	bst free_page_descs;

	// total number of pages in free_pages_desc
	uint64_t free_pages_count;

	// there are 2 maps that store page_desc
	// this allows us to get pages both using page_id (to acquire locks) and page_memory (to free and upgrade/downgrade locks)

	// page_id -> page_desc
	hashmap page_id_map;

	// page_memory -> page_desc
	hashmap page_memory_map;

	// total number of pages in the system = total_page
	// total_pages = get_element_count_hashmap(page_id_map)
	// the number of pages in page_memory_map = total_pages - free_pages_count
};

#define MIN_BUCKET_COUNT 128

static int open_data_file(void* context)
{
	memory_store_context* cntxt = context;

	pthread_mutex_init(&(cntxt->global_lock), NULL);
	cntxt->free_pages_count = 0;
	initialize_bst(&(cntxt->free_page_descs), RED_BLACK_TREE, &simple_comparator(compare_page_descs_by_page_ids), offsetof(page_descriptor, free_page_descs_node));
	initialize_hashmap(&(cntxt->page_id_map), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, MIN_BUCKET_COUNT, &simple_hasher(hash_on_page_id), &simple_comparator(compare_page_descs_by_page_ids), offsetof(page_descriptor, page_id_map_node));
	initialize_hashmap(&(cntxt->page_memory_map), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, MIN_BUCKET_COUNT, &simple_hasher(hash_on_page_memory), &simple_comparator(compare_page_descs_by_page_memories), offsetof(page_descriptor, page_memory_map_node));

	return 1;
}

static void* get_new_page_with_write_lock(void* context, uint64_t* page_id_returned)
{
	memory_store_context* cntxt = context;

	void* page_ptr = NULL;

	pthread_mutex_lock(&(cntxt->global_lock));

		// get page_descriptor from free_page_descs, with the lowest page_id
		page_descriptor* page_desc = (page_descriptor*)find_smallest_in_bst(&(cntxt->free_page_descs));

		if(page_desc != NULL)
		{
			// if a page_desc is found, then then remove it from the free_page_descs
			remove_from_bst(&(cntxt->free_page_descs), page_desc);
		}
		else
		{
			if(get_element_count_hashmap(&(cntxt->page_id_map)) < cntxt->MAX_PAGE_COUNT)
			{
				// create a new page_descriptor for the new unseen page
				page_desc = get_new_page_descriptor(get_element_count_hashmap(&(cntxt->page_id_map)), &(cntxt->global_lock));

				// insert this new page descriptor in the page_id_map
				insert_in_hashmap(&(cntxt->page_id_map), page_desc);
			}
		}

		// we have a valid BUT free page descriptor
		if(page_desc != NULL)
		{
			// allocate page memory for this free page descriptor
			page_desc->page_memory = allocate_page(cntxt->page_size);

			// TODO : ROLLBACK, if allocation fails

			// this page_descriptor has page_memory associated with it, hopefully if the call to malloc doesn't fail
			page_desc->is_free = 0;

			// insert this page in the page_memory_map
			insert_in_hashmap(&(cntxt->page_memory_map), page_desc);

			// get write lock on this page, this call will not fail here at all
			write_lock(&(page_desc->page_lock), BLOCKING);

			// assign return values
			page_ptr = page_desc->page_memory;
			*page_id_returned = page_desc->page_id;
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return page_ptr;
}

static void* acquire_page_with_reader_lock(void* context, uint64_t page_id)
{
	memory_store_context* cntxt = context;

	void* page_ptr = NULL;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_id_map), &((page_descriptor){.page_id = page_id}));

		// attempt to acquire a lock if such a page_descriptor exists and is not free
		if(page_desc != NULL && (!(page_desc->is_free)))
		{
			int lock_acquired = read_lock(&(page_desc->page_lock), READ_PREFERRING, BLOCKING);

			if(lock_acquired)
			{
				// page could have been freed while we were blocked for the lock
				if(page_desc->is_free)
					read_unlock(&(page_desc->page_lock));
				else
					page_ptr = page_desc->page_memory;
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return page_ptr;
}

static void* acquire_page_with_writer_lock(void* context, uint64_t page_id)
{
	memory_store_context* cntxt = context;

	void* page_ptr = NULL;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_id_map), &((page_descriptor){.page_id = page_id}));

		// attempt to acquire a lock if such a page_descriptor exists and is not free
		if(page_desc != NULL && (!page_desc->is_free) && (!page_desc->is_marked_for_freeing))
		{
			int lock_acquired = write_lock(&(page_desc->page_lock), NON_BLOCKING);
			
			if(lock_acquired)
			{
				// page could have been freed while we were blocked for the lock
				if(page_desc->is_free)
					write_unlock(&(page_desc->page_lock));
				else
					page_ptr = page_desc->page_memory;
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return page_ptr;
}

static int downgrade_writer_lock_to_reader_lock_on_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	int lock_downgraded = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
			lock_downgraded = downgrade_lock(&(page_desc->page_lock));

	pthread_mutex_unlock(&(cntxt->global_lock));

	return lock_downgraded;
}

static int delete_trailing_free_page_descs_unsafe(memory_store_context* cntxt)
{
	int page_count_shrunk = 0;

	// loop to delete trailing page_descriptors
	while(1)
	{
		page_descriptor* trailing = (page_descriptor*)find_largest_in_bst(&(cntxt->free_page_descs));

		// check if this page_descriptor is a trailing (the last of page_descriptors)
		if(trailing != NULL && trailing->page_id + 1 == cntxt->total_pages_count)
		{
			// remove the trailing page_descriptor, it will not exist in page_memory_map, since it is a free page
			remove_from_bst(&(cntxt->free_page_descs), trailing);
			remove_from_hashmap(&(cntxt->page_id_map), trailing);

			// now we can safely delete the page_descriptor
			delete_page_descriptor(trailing);
			trailing = NULL; // discarding dangling local pointer

			// mark true that the total_pages_count was shrunk
			page_count_shrunk = 1;
		}
		else
			break;
	}

	return page_count_shrunk;
}

// returns 1, if this call ensures you that the waiting threads are now 0
static int wait_until_waiting_threads_leave_if_marked_for_free_unsafe(memory_store_context* cntxt, page_descriptor* page_desc)
{
	if(page_desc->is_free || !page_desc->is_marked_for_freeing)
		return 0;

	// wait loop until there are no threads waiting for lock
	while(page_desc->waiting_threads > 0)
	{
		// wake up all the waiting threads
		pthread_cond_broadcast(&(page_desc->block_wait));

		// wait until the last one leaves
		pthread_cond_wait(&(page_desc->free_wait), &(cntxt->global_lock));
	}

	return 1;
}

static int free_page_if_marked_for_free_unsafe(memory_store_context* cntxt, page_descriptor* page_desc)
{
	// if the page_desc is already free OR is not marked for freeing then return with a failure
	if(page_desc->is_free || !page_desc->is_marked_for_freeing)
		return 0;

	// free, only if there are no threads holding locks on this page, AND no thread is waiting to release locks on this page
	if(page_desc->reading_threads == 0 && page_desc->writing_threads == 0 && page_desc->waiting_threads == 0)
	{
		// remove page_desc from page_memory_map
		remove_from_hashmap(&(cntxt->page_memory_map), page_desc);

		// deallocate page and mark page for freeing
		deallocate_page(page_desc->page_memory);
		page_desc->page_memory = NULL;
		page_desc->is_free = 1;
		page_desc->is_marked_for_freeing = 0;

		// insert page_desc in free_page_descs
		insert_in_bst(&(cntxt->free_page_descs), page_desc);

		// delete trailing page_descriptors
		delete_trailing_free_page_descs_unsafe(cntxt);

		return 1;
	}
	
	return 0;
}

static int release_writer_lock_on_page(void* context, void* pg_ptr, int was_modified)
{
	memory_store_context* cntxt = context;

	int lock_released = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
		{
			lock_released = release_write_lock_on_page_memory_unsafe(page_desc);

			// if lock was released and the page was marked for freeing and there are no threads who currently hold any locks
			// then attempt freeing the page
			if(lock_released && page_desc->is_marked_for_freeing)
			{
				if(page_desc->writing_threads == 0 && page_desc->reading_threads == 0) // this condition must be true, due to exclusive nature of write lock
				{
					wait_until_waiting_threads_leave_if_marked_for_free_unsafe(cntxt, page_desc);

					// free the page, if it was amrked to be free while we held the lock
					free_page_if_marked_for_free_unsafe(cntxt, page_desc);
				}
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return lock_released;
}

static int release_reader_lock_on_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	int lock_released = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
		{
			lock_released = release_read_lock_on_page_memory_unsafe(page_desc);

			// if lock was released and the page was marked for freeing and there are no threads who currently hold any locks
			// then attempt freeing the page
			if(lock_released && page_desc->is_marked_for_freeing)
			{
				if(page_desc->writing_threads == 0 && page_desc->reading_threads == 0)
				{
					wait_until_waiting_threads_leave_if_marked_for_free_unsafe(cntxt, page_desc);

					// free the page, if it was amrked to be free while we held the lock
					free_page_if_marked_for_free_unsafe(cntxt, page_desc);
				}
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return lock_released;
}

static int release_writer_lock_and_free_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	int lock_released_and_will_be_freed = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc != NULL)
		{
			lock_released_and_will_be_freed = release_write_lock_on_page_memory_unsafe(page_desc);

			// only if page lock was released, then we move further to free the page or mark it free
			if(lock_released_and_will_be_freed)
			{
				// if the page is not marked for freeing, then mark it for freeing
				if(!page_desc->is_marked_for_freeing)
				{
					// mark page for freeing
					page_desc->is_marked_for_freeing = 1;

					// wake up all the waiting threads, if there are any
					if(page_desc->waiting_threads > 0)
						pthread_cond_broadcast(&(page_desc->block_wait));
				}

				// here, we are sure that the page, has been marked for freeing

				// go ahead with freeing the page only if, there are no other readers or writers to this page
				if(page_desc->writing_threads == 0 && page_desc->reading_threads == 0)
				{
					// wait until there are no threads waiting for lock
					wait_until_waiting_threads_leave_if_marked_for_free_unsafe(cntxt, page_desc);

					// free the page, if the page was not freed while we waited on condition variable
					free_page_if_marked_for_free_unsafe(cntxt, page_desc);
				}
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return lock_released_and_will_be_freed;
}

static int release_reader_lock_and_free_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	int lock_released_and_will_be_freed = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc != NULL)
		{
			lock_released_and_will_be_freed = release_read_lock_on_page_memory_unsafe(page_desc);

			// only if page lock was released, then we move further to free the page or mark it free
			if(lock_released_and_will_be_freed)
			{
				// if the page is not marked for freeing, then mark it for freeing
				if(!page_desc->is_marked_for_freeing)
				{
					// mark page for freeing
					page_desc->is_marked_for_freeing = 1;

					// wake up all the waiting threads, if there are any
					if(page_desc->waiting_threads > 0)
						pthread_cond_broadcast(&(page_desc->block_wait));
				}

				// here, we are sure that the page, has been marked for freeing

				// go ahead with freeing the page only if, there are no other readers or writers to this page
				if(page_desc->writing_threads == 0 && page_desc->reading_threads == 0)
				{
					// wait until there are no threads waiting for lock
					wait_until_waiting_threads_leave_if_marked_for_free_unsafe(cntxt, page_desc);

					// free the page, if the page was not freed while we waited on condition variable
					free_page_if_marked_for_free_unsafe(cntxt, page_desc);
				}
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return lock_released_and_will_be_freed;
}

int free_page(void* context, uint64_t page_id)
{
	memory_store_context* cntxt = context;

	int will_be_freed = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_id_map), &((page_descriptor){.page_id = page_id}));

		// if the page_desc exists and is not free and is not marked for freeing
		if(page_desc != NULL && !page_desc->is_free && !page_desc->is_marked_for_freeing)
		{
			// if the page is not marked for freeing, then mark it for freeing
			if(!page_desc->is_marked_for_freeing) // this condition is redundant
			{
				// mark page for freeing
				page_desc->is_marked_for_freeing = 1;

				// wake up all the waiting threads, if there are any
				if(page_desc->waiting_threads > 0)
					pthread_cond_broadcast(&(page_desc->block_wait));
			}

			// here, we are sure that the page, has been marked for freeing

			// go ahead with freeing the page only if, there are no other readers or writers to this page
			if(page_desc->writing_threads == 0 && page_desc->reading_threads == 0)
			{
				// wait until there are no threads waiting for lock
				wait_until_waiting_threads_leave_if_marked_for_free_unsafe(cntxt, page_desc);

				// free the page, if the page was not freed while we waited on condition variable
				free_page_if_marked_for_free_unsafe(cntxt, page_desc);
			}

			will_be_freed = 1;
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	return will_be_freed;
}

uint64_t get_page_id_for_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	uint64_t page_id = cntxt->MAX_PAGE_COUNT;

	pthread_mutex_lock(&(cntxt->global_lock));

		const page_descriptor* page_desc = find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
			page_id = page_desc->page_id;

	pthread_mutex_unlock(&(cntxt->global_lock));

	return page_id;
}

static int force_write_to_disk(void* context, uint64_t page_id){ /* NOOP */ return 1;}

static void delete_page_descriptor_hashmap_operation(const void* data, const void* additional_params)
{
	if(((page_descriptor*)(data))->page_memory != NULL)
		deallocate_page(((page_descriptor*)(data))->page_memory);
	delete_page_descriptor(((page_descriptor*)(data)));
}

static int close_data_file(void* context)
{
	memory_store_context* cntxt = context;

	for_each_in_hashmap(&(cntxt->page_id_map), delete_page_descriptor_hashmap_operation, NULL);

	deinitialize_hashmap(&(cntxt->page_id_map));
	deinitialize_hashmap(&(cntxt->page_memory_map));
	pthread_mutex_destroy(&(cntxt->global_lock));

	cntxt->free_pages_count = 0;
	cntxt->total_pages_count = 0;

	return 1;
}

data_access_methods* get_new_in_memory_data_store(uint32_t page_size, uint8_t page_id_width)
{
	// check for invalud page_width
	if(page_id_width == 0 || page_id_width > 8)
		return NULL;

	data_access_methods* dam_p = malloc(sizeof(data_access_methods));

	dam_p->open_data_file = open_data_file;
	dam_p->get_new_page_with_write_lock = get_new_page_with_write_lock;
	dam_p->acquire_page_with_reader_lock = acquire_page_with_reader_lock;
	dam_p->acquire_page_with_writer_lock = acquire_page_with_writer_lock;
	dam_p->downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page;
	dam_p->release_reader_lock_on_page = release_reader_lock_on_page;
	dam_p->release_writer_lock_on_page = release_writer_lock_on_page;
	dam_p->release_writer_lock_and_free_page = release_writer_lock_and_free_page;
	dam_p->release_reader_lock_and_free_page = release_reader_lock_and_free_page;
	dam_p->free_page = free_page;
	dam_p->get_page_id_for_page = get_page_id_for_page;
	dam_p->force_write_to_disk = force_write_to_disk;
	dam_p->close_data_file = close_data_file;

	dam_p->page_size = page_size;
	dam_p->page_id_width = page_id_width;

	uint8_t bits_wanted1 = dam_p->page_id_width * 8;
	if(bits_wanted1 < 64)
		dam_p->NULL_PAGE_ID = (((uint64_t)1) << bits_wanted1) - ((uint64_t)1);
	else
		dam_p->NULL_PAGE_ID = -1;
	
	dam_p->context = malloc(sizeof(memory_store_context));
	
	((memory_store_context*)(dam_p->context))->page_size = page_size;
	((memory_store_context*)(dam_p->context))->MAX_PAGE_COUNT = dam_p->NULL_PAGE_ID;

	// on success return the data access methods
	if(dam_p->open_data_file(dam_p->context))
		return dam_p;

	// on error return NULL
	free(dam_p->context);
	free(dam_p);
	return NULL;
}

int close_and_destroy_in_memory_data_store(data_access_methods* dam_p)
{
	int result = dam_p->close_data_file(dam_p->context);
	free(dam_p->context);
	free(dam_p);
	return result;
}