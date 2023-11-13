#include<unWALed_in_memory_data_store.h>

#include<hashmap.h>
#include<bst.h>
#include<linkedlist.h>

#include<rwlock.h>

#include<stddef.h>
#include<stdlib.h>
#include<stdio.h>

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
	if(page_desc == NULL)
		return NULL;

	page_desc->page_id = page_id;
	page_desc->is_free = 1;	// because initially page_memory = NULL
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

static void deallocate_page(void* page)
{
	free(page);
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

static int discard_trailing_free_page_descs_unsafe(memory_store_context* cntxt)
{
	int page_count_shrunk = 0;

	// loop to delete trailing page_descriptors
	page_descriptor* trailing = (page_descriptor*)find_largest_in_bst(&(cntxt->free_page_descs));
	while(trailing != NULL)
	{
		// if trailing is not the last page based on its id, then break
		if(trailing->page_id != get_element_count_hashmap(&(cntxt->page_id_map)) - 1)
			break;

		// the page_desc with second largest page_id in free_page_desc
		page_descriptor* trailing_prev = (page_descriptor*)get_inorder_prev_of_in_bst(&(cntxt->free_page_descs), trailing);

		// remove the trailing page_descriptor, it will not exist in page_memory_map, since it is a free page
		// we also do not need to deallocate the page_memory, since we found this page in the free_page_descs
		cntxt->free_pages_count -= remove_from_bst(&(cntxt->free_page_descs), trailing);
		remove_from_hashmap(&(cntxt->page_id_map), trailing);

		// now we can safely delete the page_descriptor
		delete_page_descriptor(trailing);

		// mark true that the total_pages_count was shrunk
		page_count_shrunk = 1;

		// prepare for next iteration
		trailing = trailing_prev;
	}

	return page_count_shrunk;
}

// call this function, only after releasing lock that you are holding on it
static int run_free_page_management_unsafe(memory_store_context* cntxt, page_descriptor* page_desc)
{
	int freed = 0;

	// if the page is read or write locked, then fail the free call
	if(is_read_locked(&(page_desc->page_lock)) || is_write_locked(&(page_desc->page_lock)))
	{
		freed = 0;
		return freed;
	}

	// if not free, mark it as free
	if(!(page_desc->is_free))
	{
		page_desc->is_free = 1; // this will ensure that, any waiters will fail to acquire a lock on this page
			// yet this page_desc and its page_id can not be reused until, it gets put back into the free_page_desc (with its page_memeory freed)
			// this will happen (if this function is called properly), when all the locks on that page are released
		freed = 1;

		// if the page has allocated page memory, and ofcourse it is not read or write locked, then release the held memory
		if(page_desc->page_memory != NULL)
		{
			// if it is not read or write locked, then it is not going to be accessed with it's page_memeory
			// remove it from page_memory_map
			remove_from_hashmap(&(cntxt->page_memory_map), page_desc);

			// deallocate page_memory
			deallocate_page(page_desc->page_memory);
			page_desc->page_memory = NULL;
		}
	}

	// if the page_desc does not exist in the free_page_desc and it is not referenced any waiters
	// then you may add it to the free_page_descs bst, so that it can be allocated again
	// we can not re-allocate the page while there are threads waiting on acquiring lock on it
	// and if there are any waiters, then it is their responsibility to insert the free page into free_pag_descs
	if(is_free_floating_bstnode(&(page_desc->free_page_descs_node)) && !has_waiters(&(page_desc->page_lock)))
	{
		// insert it into the free_page_descs, this ensures, that this page_desc can be reused by a get_new_page_with_write_lock call
		cntxt->free_pages_count += insert_in_bst(&(cntxt->free_page_descs), page_desc);

		// delete trailing free_pages from free_page_descs
		discard_trailing_free_page_descs_unsafe(cntxt);
	}

	return freed;
}

static void* get_new_page_with_write_lock(void* context, const void* transaction_id, uint64_t* page_id_returned, int* abort_error)
{
	memory_store_context* cntxt = context;

	void* page_ptr = NULL;

	pthread_mutex_lock(&(cntxt->global_lock));

		// get page_descriptor from free_page_descs, with the lowest page_id
		page_descriptor* page_desc = (page_descriptor*)find_smallest_in_bst(&(cntxt->free_page_descs));

		if(page_desc != NULL)
		{
			// if a page_desc is found, then then remove it from the free_page_descs
			cntxt->free_pages_count -= remove_from_bst(&(cntxt->free_page_descs), page_desc);
		}
		else
		{
			if(get_element_count_hashmap(&(cntxt->page_id_map)) < cntxt->MAX_PAGE_COUNT)
			{
				// create a new page_descriptor for the new unseen page
				page_desc = get_new_page_descriptor(get_element_count_hashmap(&(cntxt->page_id_map)), &(cntxt->global_lock));

				// insert this new page descriptor in the page_id_map (,if it was allocated and initialized)
				if(page_desc)
					insert_in_hashmap(&(cntxt->page_id_map), page_desc);
			}
		}

		// we have a valid BUT free page descriptor
		if(page_desc != NULL)
		{
			// allocate page memory for this free page descriptor
			page_desc->page_memory = allocate_page(cntxt->page_size);

			if(page_desc->page_memory != NULL)
			{
				// this page_descriptor has page_memory associated with it, hopefully if the call to malloc doesn't fail
				page_desc->is_free = 0;

				// insert this page in the page_memory_map, so that they future calls to release or upgrade/downgrade lock calls can find it
				insert_in_hashmap(&(cntxt->page_memory_map), page_desc);

				// get write lock on this page, this call will not fail here at all
				write_lock(&(page_desc->page_lock), BLOCKING);

				// assign return values
				page_ptr = page_desc->page_memory;
				*page_id_returned = page_desc->page_id;
			}
			else // ROLLBACK, if allocation fails
			{
				// insert it back into free_page_descs, it has no page_memory
				cntxt->free_pages_count += insert_in_bst(&(cntxt->free_page_descs), page_desc);

				// delete trailing free_pages from free_page_descs
				discard_trailing_free_page_descs_unsafe(cntxt);
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(page_ptr == NULL)
		(*abort_error) = 1;

	return page_ptr;
}

static void* acquire_page_with_reader_lock(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
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
				{
					read_unlock(&(page_desc->page_lock));

					run_free_page_management_unsafe(cntxt, page_desc);
				}
				else
					page_ptr = page_desc->page_memory;
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(page_ptr == NULL)
		(*abort_error) = 1;

	return page_ptr;
}

static void* acquire_page_with_writer_lock(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	memory_store_context* cntxt = context;

	void* page_ptr = NULL;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_id_map), &((page_descriptor){.page_id = page_id}));

		// attempt to acquire a lock if such a page_descriptor exists and is not free
		if(page_desc != NULL && (!(page_desc->is_free)))
		{
			int lock_acquired = write_lock(&(page_desc->page_lock), BLOCKING);
			
			if(lock_acquired)
			{
				// page could have been freed while we were blocked for the lock
				if(page_desc->is_free)
				{
					write_unlock(&(page_desc->page_lock));

					run_free_page_management_unsafe(cntxt, page_desc);
				}
				else
					page_ptr = page_desc->page_memory;
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(page_ptr == NULL)
		(*abort_error) = 1;

	return page_ptr;
}

static int downgrade_writer_lock_to_reader_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	memory_store_context* cntxt = context;

	int lock_downgraded = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
			lock_downgraded = downgrade_lock(&(page_desc->page_lock));

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(lock_downgraded == 0)
		(*abort_error) = 1;

	return lock_downgraded;
}

static int upgrade_reader_lock_to_writer_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int* abort_error)
{
	memory_store_context* cntxt = context;

	int lock_upgraded = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
			lock_upgraded = upgrade_lock(&(page_desc->page_lock), BLOCKING);

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(lock_upgraded == 0)
		(*abort_error) = 1;

	return lock_upgraded;
}

static int release_writer_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	memory_store_context* cntxt = context;

	int lock_released = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
		{
			lock_released = write_unlock(&(page_desc->page_lock));

			// the page would never have been freed while we were having write lock in it, so we do not need to worry about this case
			if(lock_released && (opts & FREE_PAGE))
			{
				int freed = run_free_page_management_unsafe(cntxt, page_desc);

				// if the page was not freed, (this may be because of other thread having lock on it, this will never happen in case of writer lock, but this code serves as a reminder of what needs to be done)
				// then we need to undo the released lock
				if(!freed)
				{
					// we know we had a write lock on it, so we take that lock back NON_BLOCKING-ly
					write_lock(&(page_desc->page_lock), NON_BLOCKING);
					lock_released = 0;
				}
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(lock_released == 0)
		(*abort_error) = 1;

	return lock_released;
}

static int release_reader_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	memory_store_context* cntxt = context;

	int lock_released = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
		{
			lock_released = read_unlock(&(page_desc->page_lock));

			// the page would never have been freed while we were having read lock in it, so we do not need to worry about this case
			if(lock_released && (opts & FREE_PAGE))
			{
				int freed = run_free_page_management_unsafe(cntxt, page_desc);

				// if the page was not freed, (this may be because of other readers having lock on it)
				// then we need to undo the released lock
				if(!freed)
				{
					// we know we had a read lock on it, so we take that lock back READ_PREFERRING-ly and NON_BLOCKING-ly
					read_lock(&(page_desc->page_lock), READ_PREFERRING, NON_BLOCKING);
					lock_released = 0;
				}
			}
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(lock_released == 0)
		(*abort_error) = 1;

	return lock_released;
}

static int free_page(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	memory_store_context* cntxt = context;

	int is_freed = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_id_map), &((page_descriptor){.page_id = page_id}));

		// if the page_desc exists and is not free
		if(page_desc != NULL && !page_desc->is_free)
			// run_free_page_management, will take care of everything
			is_freed = run_free_page_management_unsafe(cntxt, page_desc);

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(is_freed == 0)
		(*abort_error) = 1;

	return is_freed;
}

data_access_methods* get_new_unWALed_in_memory_data_store(uint32_t page_size, uint8_t page_id_width)
{
	// check for invalud page_width
	if(page_id_width == 0 || page_id_width > sizeof(uint64_t))
		return NULL;

	data_access_methods* dam_p = malloc(sizeof(data_access_methods));
	if(dam_p == NULL)
		return NULL;

	dam_p->get_new_page_with_write_lock = get_new_page_with_write_lock;
	dam_p->acquire_page_with_reader_lock = acquire_page_with_reader_lock;
	dam_p->acquire_page_with_writer_lock = acquire_page_with_writer_lock;
	dam_p->downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page;
	dam_p->upgrade_reader_lock_to_writer_lock_on_page = upgrade_reader_lock_to_writer_lock_on_page;
	dam_p->release_reader_lock_on_page = release_reader_lock_on_page;
	dam_p->release_writer_lock_on_page = release_writer_lock_on_page;
	dam_p->free_page = free_page;

	dam_p->page_size = page_size;
	dam_p->page_id_width = page_id_width;

	dam_p->NULL_PAGE_ID = UINT64_MAX >> ((sizeof(uint64_t) - dam_p->page_id_width) * CHAR_BIT);
	
	dam_p->context = malloc(sizeof(memory_store_context));
	if(dam_p->context == NULL)
	{
		free(dam_p);
		return NULL;
	}
	
	((memory_store_context*)(dam_p->context))->page_size = page_size;
	((memory_store_context*)(dam_p->context))->MAX_PAGE_COUNT = dam_p->NULL_PAGE_ID;

	// on success return the data access methods

	memory_store_context* cntxt = dam_p->context;

	pthread_mutex_init(&(cntxt->global_lock), NULL);
	cntxt->free_pages_count = 0;
	initialize_bst(&(cntxt->free_page_descs), RED_BLACK_TREE, &simple_comparator(compare_page_descs_by_page_ids), offsetof(page_descriptor, free_page_descs_node));
	if(!initialize_hashmap(&(cntxt->page_id_map), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, MIN_BUCKET_COUNT, &simple_hasher(hash_on_page_id), &simple_comparator(compare_page_descs_by_page_ids), offsetof(page_descriptor, page_id_map_node)))
	{
		free(dam_p->context);
		free(dam_p);
		return NULL;
	}
	if(!initialize_hashmap(&(cntxt->page_memory_map), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, MIN_BUCKET_COUNT, &simple_hasher(hash_on_page_memory), &simple_comparator(compare_page_descs_by_page_memories), offsetof(page_descriptor, page_memory_map_node)))
	{
		deinitialize_hashmap(&(cntxt->page_id_map));
		free(dam_p->context);
		free(dam_p);
		return NULL;
	}
	return dam_p;
}

static void delete_notified_page_descriptor(void* resource_p, const void* data)
{
	if(((page_descriptor*)(data))->page_memory != NULL)
		deallocate_page(((page_descriptor*)(data))->page_memory);
	delete_page_descriptor(((page_descriptor*)(data)));
}

int close_and_destroy_unWALed_in_memory_data_store(data_access_methods* dam_p)
{
	memory_store_context* cntxt = dam_p->context;
	printf("pages still being used = %llu, of which %lu are free\n", get_element_count_hashmap(&(cntxt->page_id_map)), cntxt->free_pages_count);
	remove_all_from_hashmap(&(cntxt->page_id_map), &((notifier_interface){NULL, delete_notified_page_descriptor}));
	deinitialize_hashmap(&(cntxt->page_id_map));
	deinitialize_hashmap(&(cntxt->page_memory_map));
	pthread_mutex_destroy(&(cntxt->global_lock));
	cntxt->free_pages_count = 0;
	free(dam_p->context);
	free(dam_p);
	return 1;
}