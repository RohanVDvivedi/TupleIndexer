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
	uint64_t page_id;

	// if free then the page is only present in 
	int is_free;

	int is_marked_for_freeing;

	// this will be NULL for a free page_desc
	void* page_memory;

	// below are the main attributes that enable locking

	uint32_t reading_threads;

	uint32_t writing_threads;

	uint32_t readers_waiting;

	uint32_t writers_waiting;

	pthread_cond_t reader_waiting;

	pthread_cond_t writer_waiting;

	// below are the 2 embedded nodes used by page_id_map and page_memory_map

	llnode page_id_map_node;

	llnode page_memory_map_node;

	// embedded node for free_page_descs

	bstnode free_page_descs_node;
};

int compare_by_page_ids(const void* page_desc1, const void* page_desc2)
{
	if(((const page_descriptor*)(page_desc1))->page_id == ((const page_descriptor*)(page_desc1))->page_id)
		return 0;
	else if(((const page_descriptor*)(page_desc1))->page_id > ((const page_descriptor*)(page_desc1))->page_id)
		return 1;
	else
		return -1;
}

unsigned int hash_on_page_id(const void* page_desc1)
{
	return (unsigned int)(((const page_descriptor*)(page_desc1))->page_id);
}

unsigned int hash_on_page_memory(const void* page_desc1)
{
	return (unsigned int)(((const page_descriptor*)(page_desc1))->page_memory);
}

typedef struct memory_store_context memory_store_context;
struct memory_store_context
{
	pthread_mutex_t global_lock;

	// constant
	uint32_t page_size;

	// MAX_PAGE_ID, no page_id will be allotted to any page above this number
	// constant
	uint64_t MAX_PAGE_ID;

	// max page_id that this system has not yet seen
	// page_descriptor for any page with page_id greater than this is not in the system
	uint64_t max_un_seen_page_id;

	// bst of free_pages, ordered by their page_ids
	// this pages are are free (they dont have their page_memory populated)
	// we always allocate new page from the least page_id from free_page_descs
	// and release memory of greatest page_descs to OS, if it is equal to the max_un_seen_page_id - 1
	bst free_page_descs;

	// total number of pages in free_pages_desc
	uint64_t free_pages_count;

	// total number of pages in the system
	// the number of pages in page_id_map = total_pages
	uint64_t total_pages_count;

	// there are 2 maps that store page_desc
	// this allows us to get pages both using page_id (to acquire locks) and page_memory (to free locks)

	// page_id -> page_desc
	hashmap page_id_map;

	// page_memeory -> page_desc
	hashmap page_memory_map;
};

#define MIN_BUCKET_COUNT 128

static int open_data_file(void* context)
{
	memory_store_context* cntxt = context;

	pthread_mutex_init(&(cntxt->global_lock), NULL);
	cntxt->max_un_seen_page_id = 0;
	cntxt->free_pages_count = 0;
	cntxt->total_pages_count = 0;
	initialize_bst(&(cntxt->free_page_descs), RED_BLACK_TREE, compare_by_page_ids, offsetof(page_descriptor, free_page_descs_node));
	initialize_hashmap(&(cntxt->page_id_map), ELEMENTS_AS_LINKEDLIST, MIN_BUCKET_COUNT, hash_on_page_id, compare_by_page_ids, offsetof(page_descriptor, page_id_map_node));
	initialize_hashmap(&(cntxt->page_memory_map), ELEMENTS_AS_LINKEDLIST, MIN_BUCKET_COUNT, hash_on_page_memory, compare_by_page_ids, offsetof(page_descriptor, page_memory_map_node));

	return 1;
}

static void* get_new_page_with_write_lock(void* context, uint64_t* page_id_returned)
{
	memory_store_context* cntxt = context;

	return NULL;
}

static void* acquire_page_with_reader_lock(void* context, uint64_t page_id)
{
	memory_store_context* cntxt = context;

	return NULL;
}

static void* acquire_page_with_writer_lock(void* context, uint64_t page_id)
{
	memory_store_context* cntxt = context;

	return NULL;
}

static int downgrade_writer_lock_to_reader_lock_on_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	return 0;
}

static int release_reader_lock_on_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	return 0;
}

static int release_writer_lock_on_page(void* context, void* pg_ptr, int was_modified)
{
	memory_store_context* cntxt = context;

	return 0;
}

static int release_writer_lock_and_free_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	return 0;
}

static int release_reader_lock_and_free_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	return 0;
}

uint64_t get_page_id_for_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	return 0;
}

static int force_write_to_disk(void* context, uint64_t page_id){ /* NOOP */ return 1;}

static int close_data_file(void* context)
{
	memory_store_context* cntxt = context;

	pthread_mutex_destroy(&(cntxt->global_lock));
	cntxt->max_un_seen_page_id = 0;
	cntxt->free_pages_count = 0;
	cntxt->total_pages_count = 0;
	deinitialize_hashmap(&(cntxt->page_id_map));
	deinitialize_hashmap(&(cntxt->page_memory_map));

	return 1;
}

data_access_methods* get_new_in_memory_data_store(uint32_t page_size, uint8_t page_id_width)
{
	// check for invalud page_width
	if(!(page_id_width == 1 || page_id_width == 2 || page_id_width == 4 || page_id_width == 8))
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
	dam_p->get_page_id_for_page = get_page_id_for_page;
	dam_p->force_write_to_disk = force_write_to_disk;
	dam_p->close_data_file = close_data_file;

	dam_p->page_size = page_size;
	dam_p->page_id_width = page_id_width;
	switch(page_id_width)
	{
		case 1:
		{
			dam_p->NULL_PAGE_ID = ((uint8_t)(-1));	// NULL_PAGE_ID is 0xff
			break;
		}
		case 2:
		{
			dam_p->NULL_PAGE_ID = ((uint16_t)(-1));	// NULL_PAGE_ID is 0xffff
			break;
		}
		case 4:
		{
			dam_p->NULL_PAGE_ID = ((uint32_t)(-1));	// NULL_PAGE_ID is 0xffffffff 
			break;
		}
		case 8:
		default:
		{
			dam_p->NULL_PAGE_ID = ((uint64_t)(-1));	// NULL_PAGE_ID is 0xffffffffffffffff
			break;
		}
	}
	dam_p->context = malloc(sizeof(memory_store_context));
	
	((memory_store_context*)(dam_p->context))->page_size = page_size;
	((memory_store_context*)(dam_p->context))->MAX_PAGE_ID = dam_p->NULL_PAGE_ID;

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