#ifndef ON_DISK_DATA_STORE
#define ON_DISK_DATA_STORE

#include<data_access_methods.h>

// page_size must be multiple of 4096 (i.e. size of a page that can be mmaped)

void get_new_on_disk_data_store(data_access_methods* dam_p, uint32_t page_size);

void close_and_destroy_on_disk_data_store(data_access_methods* dam_p);

#endif