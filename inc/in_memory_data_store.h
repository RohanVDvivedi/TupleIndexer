#ifndef IN_MEMORY_DATA_STORE_H
#define IN_MEMORY_DATA_STORE_H

#include<data_access_methods.h>

// page_size must be multiple of 4096 (i.e. size of a page that can be mmaped)

void get_new_in_memory_data_store(data_access_methods* dam_p, uint32_t page_size);

void destroy_in_memory_data_store(data_access_methods* dam_p);

#endif