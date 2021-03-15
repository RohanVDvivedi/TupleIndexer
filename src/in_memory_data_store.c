#include<in_memory_data_store.h>

#include<rwlock.h>

#include<hashmap.h>
#include<linkedlist.h>

#include<stdlib.h>
#include<sys/mman.h>

#define IS_FREE_FLAG 0x00000001

typedef struct page_state page_state;
struct page_state
{
	int flags;

	rwlock reader_writer_page_lock;
};

typedef struct memory_store_context memory_store_context;
struct memory_store_context
{
	// constant
	uint32_t page_size;

	// constant
	uint32_t pages_count;

	// count
	void* memory;	// points to mmap-ed memory of size (page_size * page_count)

	// this lock protects free_pages_list and the flags inside each of the page_state
	pthread_mutex_t free_pages_lock;

	linkedlist free_pages;

	page_state page_states[];
};

static int open_data_file(void* context)
{
	memory_store_context* cntxt = context;

	pthread_mutex_init(&(cntxt->free_pages_lock), NULL);
	initialize_linkedlist(&(cntxt->free_pages), 0);
	cntxt->memory = mmap(NULL, cntxt->page_size * cntxt->pages_count, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE, -1, 0);
	for(uint32_t i = 0; i < cntxt->pages_count; i++)
	{
		void* mem = cntxt->memory + (cntxt->page_size * i);
		initialize_llnode(mem);
		insert_tail(&(cntxt->free_pages), mem);
		cntxt->page_states[i].flags |= IS_FREE_FLAG;
		initialize_rwlock(&(cntxt->page_states[i].reader_writer_page_lock));
	}

	return 1;
}

static void* get_new_page_with_write_lock(void* context, uint32_t* page_id_returned)
{
	memory_store_context* cntxt = context;

	// we have to find this free page and return it
	void* free_page_p = NULL;
	uint32_t free_page_id = 0;

	pthread_mutex_lock(&(cntxt->free_pages_lock));

		// if the free_pages list is not empty, then get a free_page from it
		if(!is_empty_linkedlist(&(cntxt->free_pages)))
		{
			// get a free page from the head of the free_pages list, and remove it
			free_page_p = (void*) get_head(&(cntxt->free_pages));
			remove_head(&(cntxt->free_pages));
			
			// calculate its index
			uint32_t free_page_id = ((uintptr_t)(cntxt-> memory - free_page_p)) / cntxt->page_size;

			// clear the free flag bit
			cntxt->page_states[free_page_id].flags &= (~IS_FREE_FLAG);
		}

	pthread_mutex_unlock(&(cntxt->free_pages_lock));

	// attempt to acquire a write lock and return free_page_id, if free_page_p is found
	if(free_page_p != NULL)
	{
		(*page_id_returned) = free_page_id;

		write_lock(&(cntxt->page_states[free_page_id].reader_writer_page_lock));
	}

	return free_page_p;
}

static void* acquire_page_with_reader_lock(void* context, uint32_t page_id)
{
	memory_store_context* cntxt = context;

	// page_id is out of bounds
	if(page_id >= cntxt->pages_count)
		return 0;

	// to check if the page that you requested is not free
	// i.e. you can not directly acquire a lock to the free page (use the methof above)
	int is_free_page = 0;

	// calculate the page pointer, that you might have to return
	void* page = cntxt->memory + (page_id * cntxt->page_size);

	pthread_mutex_lock(&(cntxt->free_pages_lock));

		// check if the page is free
		is_free_page = (cntxt->page_states[page_id].flags & IS_FREE_FLAG);

	pthread_mutex_unlock(&(cntxt->free_pages_lock));

	// you may acquire the lock only if it is not free
	if(!is_free_page)
	{
		read_lock(&(cntxt->page_states[page_id].reader_writer_page_lock));

		return page;
	}
	else
		return NULL;
}

static void* acquire_page_with_writer_lock(void* context, uint32_t page_id)
{
	memory_store_context* cntxt = context;

	// page_id is out of bounds
	if(page_id >= cntxt->pages_count)
		return 0;

	// to check if the page that you requested is not free
	// i.e. you can not directly acquire a lock to the free page (use the methof above)
	int is_free_page = 0;

	// calculate the page pointer, that you might have to return
	void* page = cntxt->memory + (page_id * cntxt->page_size);

	pthread_mutex_lock(&(cntxt->free_pages_lock));

		// check if the page is free
		is_free_page = (cntxt->page_states[page_id].flags & IS_FREE_FLAG);

	pthread_mutex_unlock(&(cntxt->free_pages_lock));

	// you may acquire the lock only if it is not free
	if(!is_free_page)
	{
		write_lock(&(cntxt->page_states[page_id].reader_writer_page_lock));

		return page;
	}
	else
		return NULL;
}

static int downgrade_writer_lock_to_reader_lock_on_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	uint32_t page_id = ((uintptr_t)(cntxt->memory - pg_ptr)) / cntxt->page_size;
	downgrade_writer_to_reader_lock(&(cntxt->reader_writer_page_locks[page_id]));

	return 1;
}

static int release_reader_lock_on_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	uint32_t page_id = ((uintptr_t)(cntxt->memory - pg_ptr)) / cntxt->page_size;
	read_unlock(&(cntxt->reader_writer_page_locks[page_id]));

	return 1;
}

static int release_writer_lock_on_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	uint32_t page_id = ((uintptr_t)(cntxt->memory - pg_ptr)) / cntxt->page_size;
	write_unlock(&(cntxt->reader_writer_page_locks[page_id]));

	return 1;
}

static int release_reader_lock_and_free_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	uint32_t page_id = ((uintptr_t)(cntxt->memory - pg_ptr)) / cntxt->page_size;

	read_unlock(&(cntxt->reader_writer_page_locks[page_id]));

	pthread_mutex_lock(&(cntxt->free_pages_lock));

	insert_head(&(cntxt->free_pages), pg_ptr);

	pthread_mutex_unlock(&(cntxt->free_pages_lock));

	return 1;
}

static int release_writer_lock_and_free_page(void* context, void* pg_ptr)
{
	memory_store_context* cntxt = context;

	uint32_t page_id = ((uintptr_t)(cntxt->memory - pg_ptr)) / cntxt->page_size;

	write_unlock(&(cntxt->reader_writer_page_locks[page_id]));

	pthread_mutex_lock(&(cntxt->free_pages_lock));

	insert_head(&(cntxt->free_pages), pg_ptr);

	pthread_mutex_unlock(&(cntxt->free_pages_lock));

	return 1;
}

static int force_write_to_disk(void* context, uint32_t page_id){ /* NOOP */ return 1;}

static int close_data_file(void* context)
{
	memory_store_context* cntxt = context;

	pthread_mutex_destroy(&(cntxt->free_pages_lock));
	munmap(cntxt->memory, cntxt->page_size * cntxt->pages_count);
	for(uint32_t i = 0; i < cntxt->pages_count; i++)
		deinitialize_rwlock(&(cntxt->reader_writer_page_locks[i]));
	return 1;
}

data_access_methods* get_new_in_memory_data_store(uint32_t page_size, uint32_t pages_count)
{
	data_access_methods* dam_p = malloc(sizeof(data_access_methods));
	dam_p->open_data_file = open_data_file;
	dam_p->get_new_page_with_write_lock = get_new_page_with_write_lock;
	dam_p->acquire_page_with_reader_lock = acquire_page_with_reader_lock;
	dam_p->acquire_page_with_writer_lock = acquire_page_with_writer_lock;
	dam_p->downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page;
	dam_p->release_reader_lock_on_page = release_reader_lock_on_page;
	dam_p->release_writer_lock_on_page = release_writer_lock_on_page;
	dam_p->release_reader_lock_and_free_page = release_reader_lock_and_free_page;
	dam_p->release_writer_lock_and_free_page = release_writer_lock_and_free_page;
	dam_p->force_write_to_disk = force_write_to_disk;
	dam_p->close_data_file = close_data_file;
	dam_p->page_size = page_size;
	dam_p->context = malloc(sizeof(memory_store_context) + (pages_count * sizeof(page_state)));
	
	((memory_store_context*)(dam_p->context))->page_size = page_size;
	((memory_store_context*)(dam_p->context))->pages_count = pages_count;

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