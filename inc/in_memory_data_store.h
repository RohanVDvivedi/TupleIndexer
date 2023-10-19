#ifndef IN_MEMORY_DATA_STORE_H
#define IN_MEMORY_DATA_STORE_H

#include<data_access_methods.h>

/*
**	Constructs you a simple in memory, unWALed data store
**	it either return success or fail with error set to 1
**	it must be used for testing purposes only
**	it will not WAL log your page allocations and deallocations, but it is concurrently accessible
*/

data_access_methods* get_new_in_memory_data_store(uint32_t page_size, uint8_t page_id_width);

int close_and_destroy_in_memory_data_store(data_access_methods* dam_p);

#endif