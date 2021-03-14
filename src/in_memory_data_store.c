#include<in_memory_data_store.h>

#include<rwlock.h>

#include<hashmap.h>
#include<linkedlist.h>

#include<stdlib.h>
#include<sys/mman.h>

typedef struct memory_store_context memory_store_context;
struct memory_store_context
{
	// constant
	uint32_t page_size;

	// constant
	uint32_t pages_count;

	// count
	void* memory;	// points to mmap-ed memory of size (page_size * page_count)

	// this lock protects free_pages_list and the reader_writer_page_locks
	pthread_mutex_t context_global_lock;

	linkedlist free_pages;

	rwlock reader_writer_page_locks[];
};

static int open_data_file(void* context)
{
	memory_store_context* cntxt = context;

	pthread_mutex_init(&(cntxt->context_global_lock), NULL);
	initialize_linkedlist(&(cntxt->free_pages), 0);
	cntxt->memory = mmap(NULL, cntxt->page_size * cntxt->pages_count, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE, -1, 0);
	for(uint32_t i = 0; i < cntxt->pages_count; i++)
	{
		void* mem = cntxt->memory + (cntxt->page_size * i);
		initialize_llnode(mem);
		insert_tail(&(cntxt->free_pages), mem);
		initialize_rwlock(&(cntxt->reader_writer_page_locks[i]));
	}

	return 1;
}

static void* get_new_page_with_write_lock(void* context, uint32_t* page_id_returned);

static void* acquire_page_with_reader_lock(void* context, uint32_t page_id);

static void* acquire_page_with_writer_lock(void* context, uint32_t page_id);

static int downgrade_writer_lock_to_reader_lock_on_page(void* context, void* pg_ptr);

static int release_reader_lock_on_page(void* context, void* pg_ptr);

static int release_writer_lock_on_page(void* context, void* pg_ptr);

static int release_reader_lock_and_free_page(void* context, void* pg_ptr);

static int release_writer_lock_and_free_page(void* context, void* pg_ptr);

static int force_write_to_disk(void* context, uint32_t page_id){ /* NOOP */ return 1;}

static int close_data_file(void* context)
{
	memory_store_context* cntxt = context;

	pthread_mutex_destroy(&(cntxt->context_global_lock));
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
	dam_p->context = malloc(sizeof(memory_store_context) + (pages_count * sizeof(rwlock)));
	
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