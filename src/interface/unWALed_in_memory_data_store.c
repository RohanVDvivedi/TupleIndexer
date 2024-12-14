#include<unWALed_in_memory_data_store.h>

#include<hashmap.h>
#include<bst.h>
#include<linkedlist.h>

#include<rwlock.h>

#include<stddef.h>
#include<stdlib.h>
#include<stdio.h>

// uncomment the below line, if you want to make this data store to check for the validity of WAS_MODIFIED option flag on releasing write lock on the page
#define CHECK_WAS_MODIFIED_BIT

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

#ifdef CHECK_WAS_MODIFIED_BIT
	// the below pointer will be used to allocate page_size memory,
	// and will hold its previous value, while you modify the page_memory
	// the contents will be checked against this memory contents to check the validity of WAS_MODIFIED option flag
	void* previous_page_memory;
#endif

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
#ifdef CHECK_WAS_MODIFIED_BIT
	page_desc->previous_page_memory = NULL;
#endif
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

	// to maintain the number to read_locks and write_locks currently active
	uint64_t active_read_locks_count;
	uint64_t active_write_locks_count;
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

			#ifdef CHECK_WAS_MODIFIED_BIT
				// deallocate previous_page_memory
				deallocate_page(page_desc->previous_page_memory);
				page_desc->previous_page_memory = NULL;
			#endif
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
			memory_set(page_desc->page_memory, 0, cntxt->page_size);

			#ifdef CHECK_WAS_MODIFIED_BIT
				if(page_desc->page_memory != NULL)
					page_desc->previous_page_memory = allocate_page(cntxt->page_size);
			#endif

			if(page_desc->page_memory != NULL
			#ifdef CHECK_WAS_MODIFIED_BIT
				&& page_desc->previous_page_memory != NULL
			#endif
			)
			{
				// this page_descriptor is now not free
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
				// deallocate whatever memory we allocated, if any
				if(page_desc->page_memory != NULL)
					deallocate_page(page_desc->page_memory);
				#ifdef CHECK_WAS_MODIFIED_BIT
					if(page_desc->previous_page_memory != NULL)
						deallocate_page(page_desc->previous_page_memory);
				#endif

				// insert it back into free_page_descs, it has no page_memory
				cntxt->free_pages_count += insert_in_bst(&(cntxt->free_page_descs), page_desc);

				// delete trailing free_pages from free_page_descs
				discard_trailing_free_page_descs_unsafe(cntxt);
			}
		}

		// on success increment the active write locks count
		if(page_ptr != NULL)
			cntxt->active_write_locks_count++;

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(page_ptr == NULL)
		(*abort_error) = 1;

	// if, we took a write lock on it, so copy the previous contents to the previous_page_memory
	#ifdef CHECK_WAS_MODIFIED_BIT
		if(page_ptr != NULL)
			memory_move(page_desc->previous_page_memory, page_desc->page_memory, cntxt->page_size);
	#endif

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

		// on success increment the active read locks count
		if(page_ptr != NULL)
			cntxt->active_read_locks_count++;

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

		// on success increment the active write locks count
		if(page_ptr != NULL)
			cntxt->active_write_locks_count++;

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(page_ptr == NULL)
		(*abort_error) = 1;

	// if, we took a write lock on it, so copy the previous contents to the previous_page_memory
	#ifdef CHECK_WAS_MODIFIED_BIT
		if(page_ptr != NULL)
			memory_move(page_desc->previous_page_memory, page_desc->page_memory, cntxt->page_size);
	#endif

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

		#ifdef CHECK_WAS_MODIFIED_BIT
			// if the was_modified bit is NOT set, and the page is modified, then exit
			if(lock_downgraded && (!(opts & WAS_MODIFIED)) && memory_compare(page_desc->page_memory, page_desc->previous_page_memory, cntxt->page_size))
			{
				printf("BUG :: downgrading write lock on a page after modfication, but WAS_MODIFIED bit not set\n");
				exit(-1);
			}
		#endif

		// on success decrement the active write locks count, and increment the active read locks count
		if(lock_downgraded)
		{
			cntxt->active_write_locks_count--;
			cntxt->active_read_locks_count++;
		}

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

		// on success decrement the active read locks count, and increment the active write locks count
		if(lock_upgraded)
		{
			cntxt->active_read_locks_count--;
			cntxt->active_write_locks_count++;
		}

	pthread_mutex_unlock(&(cntxt->global_lock));

	// set error if returning failure
	if(lock_upgraded == 0)
		(*abort_error) = 1;

	// if, we took a write lock on it, so copy the previous contents to the previous_page_memory
	#ifdef CHECK_WAS_MODIFIED_BIT
		if(lock_upgraded)
			memory_move(page_desc->previous_page_memory, page_desc->page_memory, cntxt->page_size);
	#endif

	return lock_upgraded;
}

static int release_writer_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	memory_store_context* cntxt = context;

	int lock_released = 0;

	pthread_mutex_lock(&(cntxt->global_lock));

		page_descriptor* page_desc = (page_descriptor*)find_equals_in_hashmap(&(cntxt->page_memory_map), &((page_descriptor){.page_memory = pg_ptr}));

		if(page_desc)
			lock_released = write_unlock(&(page_desc->page_lock));

		#ifdef CHECK_WAS_MODIFIED_BIT
			// if the was_modified bit is NOT set, and the page is modified, then exit
			if(lock_released && (!(opts & WAS_MODIFIED)) && memory_compare(page_desc->page_memory, page_desc->previous_page_memory, cntxt->page_size))
			{
				printf("BUG :: releasing write lock on a page after modfication, but WAS_MODIFIED bit not set\n");
				exit(-1);
			}
		#endif

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

		// on success decrement the active write locks count
		if(lock_released)
			cntxt->active_write_locks_count--;

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

		// on success decrement the active read locks count
		if(lock_released)
			cntxt->active_read_locks_count--;

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

#include<page_layout_unaltered.h>

static int is_valid_page_access_specs_as_params(const page_access_specs* pas_p)
{
	// bytes required to store page id, must be between 1 and 8 both inclusive
	if(pas_p->page_id_width == 0 || pas_p->page_id_width > 8)
		return 0;

	return 1;
}

page_access_methods* get_new_unWALed_in_memory_data_store(const page_access_specs* pas_suggested)
{
	if(!is_valid_page_access_specs_as_params(pas_suggested))
		return 0;

	page_access_methods* pam_p = malloc(sizeof(page_access_methods));
	if(pam_p == NULL)
		return NULL;

	if(!initialize_page_access_specs(&(pam_p->pas), pas_suggested->page_id_width, pas_suggested->page_size, UINT64_MAX >> ((sizeof(uint64_t) - pas_suggested->page_id_width) * CHAR_BIT)))
		return NULL;

	pam_p->get_new_page_with_write_lock = get_new_page_with_write_lock;
	pam_p->acquire_page_with_reader_lock = acquire_page_with_reader_lock;
	pam_p->acquire_page_with_writer_lock = acquire_page_with_writer_lock;
	pam_p->downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page;
	pam_p->upgrade_reader_lock_to_writer_lock_on_page = upgrade_reader_lock_to_writer_lock_on_page;
	pam_p->release_reader_lock_on_page = release_reader_lock_on_page;
	pam_p->release_writer_lock_on_page = release_writer_lock_on_page;
	pam_p->free_page = free_page;
	
	pam_p->context = malloc(sizeof(memory_store_context));
	if(pam_p->context == NULL)
	{
		free(pam_p);
		return NULL;
	}
	
	((memory_store_context*)(pam_p->context))->page_size = pam_p->pas.page_size;
	((memory_store_context*)(pam_p->context))->MAX_PAGE_COUNT = pam_p->pas.NULL_PAGE_ID;

	// on success return the data access methods

	memory_store_context* cntxt = pam_p->context;

	pthread_mutex_init(&(cntxt->global_lock), NULL);
	cntxt->free_pages_count = 0;
	initialize_bst(&(cntxt->free_page_descs), RED_BLACK_TREE, &simple_comparator(compare_page_descs_by_page_ids), offsetof(page_descriptor, free_page_descs_node));
	if(!initialize_hashmap(&(cntxt->page_id_map), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, MIN_BUCKET_COUNT, &simple_hasher(hash_on_page_id), &simple_comparator(compare_page_descs_by_page_ids), offsetof(page_descriptor, page_id_map_node)))
	{
		free(pam_p->context);
		free(pam_p);
		return NULL;
	}
	if(!initialize_hashmap(&(cntxt->page_memory_map), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, MIN_BUCKET_COUNT, &simple_hasher(hash_on_page_memory), &simple_comparator(compare_page_descs_by_page_memories), offsetof(page_descriptor, page_memory_map_node)))
	{
		deinitialize_hashmap(&(cntxt->page_id_map));
		free(pam_p->context);
		free(pam_p);
		return NULL;
	}
	cntxt->active_read_locks_count = 0;
	cntxt->active_write_locks_count = 0;
	return pam_p;
}

static void delete_notified_page_descriptor(void* resource_p, const void* data)
{
	if(((page_descriptor*)(data))->page_memory != NULL)
		deallocate_page(((page_descriptor*)(data))->page_memory);
	#ifdef CHECK_WAS_MODIFIED_BIT
		if(((page_descriptor*)(data))->previous_page_memory != NULL)
			deallocate_page(((page_descriptor*)(data))->previous_page_memory);
	#endif
	delete_page_descriptor(((page_descriptor*)(data)));
}

int close_and_destroy_unWALed_in_memory_data_store(page_access_methods* pam_p)
{
	memory_store_context* cntxt = pam_p->context;
	printf("pages still being used = %llu, of which %lu are free\n", get_element_count_hashmap(&(cntxt->page_id_map)), cntxt->free_pages_count);
	printf("active locks count, read = %lu, write %lu\n", cntxt->active_read_locks_count, cntxt->active_write_locks_count);
	remove_all_from_hashmap(&(cntxt->page_id_map), &((notifier_interface){NULL, delete_notified_page_descriptor}));
	deinitialize_hashmap(&(cntxt->page_id_map));
	deinitialize_hashmap(&(cntxt->page_memory_map));
	pthread_mutex_destroy(&(cntxt->global_lock));
	cntxt->free_pages_count = 0;
	free(pam_p->context);
	free(pam_p);
	return 1;
}