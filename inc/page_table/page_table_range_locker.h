#ifndef PAGE_TABLE_RANGE_LOCKER_H
#define PAGE_TABLE_RANGE_LOCKER_H

#include<page_table.h>
#include<persistent_page.h>
#include<page_table_tuple_definitions.h>
#include<opaque_page_access_methods.h>
#include<opaque_page_modification_methods.h>

typedef struct page_table_range_locker page_table_range_locker;
struct page_table_range_locker
{
	// page table is internally a array_table with entries being page_ids
	array_table_range_locker atrl;
};

// all functions on page_table_range_locker are declared here, in this header file
#include<page_table_range_locker_public.h>

#endif