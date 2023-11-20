#ifndef IN_MEMORY_DATA_STORE_H
#define IN_MEMORY_DATA_STORE_H

#include<page_access_specification.h>
#include<page_access_methods.h>

/*
**	Constructs you a simple in memory, unWALed data store
**	it either return success or fail with error set to 1
**	it must be used for testing purposes only
**	it will not WAL log your page allocations and deallocations, but it is concurrently accessible
*/

page_access_methods* get_new_unWALed_in_memory_data_store(const page_access_specs* pas_suggested);

int close_and_destroy_unWALed_in_memory_data_store(page_access_methods* dam_p);

#endif