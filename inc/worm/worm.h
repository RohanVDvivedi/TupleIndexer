#ifndef WORM_H
#define WORM_H

/* WORM = Write-Once-Read-Multiple-times data structure
** use it for storing larger than page data types, or types that do not fit on page
** also has a reference counter for the actual blob data in worm and for the of dependent_root_page_id, AND the worm with the dependent_root_page_id is destroyed when the reference count reaches 0
** each page has atleast 1 partial_blob_type's tuple on it
*/

/*
**	This data type could possibly used as a reference page for storing the root_page_id and the tuple_defs of lets say a bplus_tree
**	This allows reference counting for the data structure at the dependent_root_page_id and the worm's binary data (possibly tuple definition and descriptions, of lets say a bplus_tree)
*/

#include<worm_tuple_definitions_public.h>

#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

// if reference_counter == 0, then it is forced to 1
uint64_t get_new_worm(uint64_t reference_counter, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

int increment_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// performs a decrement on the worm's reference_counter, and if it reaches 0, then the worm is destroyed
int decrement_reference_counter_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// getter and setter for the dependent_root_page_id for the worm
// in an ideal situation you may not want to set the dependent_root_page_id, because this would mean you are de-attaching the reference counter from its dependent_root_page_id, which is not the intended way worm is meant to be used
// but this function is provided for convinience and completeness of this data structure
uint64_t get_dependent_root_page_id_for_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);
int set_dependent_root_page_id_for_worm(uint64_t head_page_id, uint64_t dependent_root_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void print_worm(uint64_t head_page_id, const worm_tuple_defs* wtd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

#include<worm_append_iterator_public.h>
#include<worm_read_iterator_public.h>

#endif