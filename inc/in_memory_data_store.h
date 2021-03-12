#ifndef IN_MEMORY_DATA_STORE_H
#define IN_MEMORY_DATA_STORE_H

#include<data_access_methods.h>

// page_size must be multiple of 4096 (i.e. size of a page that can be mmaped)

data_access_methods* get_new_in_memory_data_store(uint32_t page_size);

int close_and_destroy_in_memory_data_store(data_access_methods* dam_p);

#endif