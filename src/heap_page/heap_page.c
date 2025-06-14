#include<tupleindexer/heap_page/heap_page.h>

#include<tupleindexer/heap_page/heap_page_header.h>

persistent_page get_new_heap_page_with_write_lock(const page_access_specs* pas_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_heap_page(const persistent_page* ppage, const page_access_specs* pas_p);