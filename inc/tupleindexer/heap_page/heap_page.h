#ifndef HEAP_PAGE_H
#define HEAP_PAGE_H

#include<tupleindexer/common/page_access_specification.h>
#include<tupleindexer/utils/persistent_page_functions.h>

// the below function returns a is_persistent_page_NULL(), only on an error
// if the error is not an abort_error, then it is just impossible to create a heap_page out of a database page, which is only possible for very small page sizes
persistent_page get_new_heap_page_with_write_lock(const page_access_specs* pas_p, const tuple_def* tpl_d, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_heap_page(const persistent_page* ppage, const page_access_specs* pas_p, const tuple_def* tpl_d);

#endif