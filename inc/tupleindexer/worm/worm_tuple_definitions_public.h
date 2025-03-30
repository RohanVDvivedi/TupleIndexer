#ifndef WORM_TUPLE_DEFINITIONS_PUBLIC_H
#define WORM_TUPLE_DEFINITIONS_PUBLIC_H

#include<tuple.h>
#include<inttypes.h>

#include<page_access_specification.h>

typedef struct worm_tuple_defs worm_tuple_defs;
struct worm_tuple_defs
{
	// specification of all the pages in the bplus_tree
	const page_access_specs* pas_p;

	// tuple_def for all blobs in the worm
	tuple_def* partial_blob_tuple_def;
};

/*
	The maximum blob type's bytes that fit on a worm page is equal to the space allotted to all tuples on the page - additional space - min size of the blob type
*/

// initializes the attributes in worm_tuple_defs struct as per the provided parameters
// the parameter pas_p must point to the pas attribute of the data_access_method that you are using it with
// it also fails if the pas_p does not pass is_valid_page_access_specs(pas_p)
int init_worm_tuple_definitions(worm_tuple_defs* wtd_p, const page_access_specs* pas_p);

// then resets all the worm_tuple_defs struct attributes to NULL or 0
void deinit_worm_tuple_definitions(worm_tuple_defs* wtd_p);

// print worm_tuple_definitions
void print_worm_tuple_definitions(const worm_tuple_defs* wtd_p);

#endif