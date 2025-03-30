#include<tupleindexer/array_table/array_table_tuple_definitions.h>

#include<tupleindexer/array_table/array_table_page_header.h>

#include<tupleindexer/utils/persistent_page_functions.h>

#include<cutlery/cutlery_math.h>

#include<stdlib.h>

uint32_t get_maximum_array_table_record_size(const page_access_specs* pas_p)
{
	array_table_tuple_defs attd = {.pas_p = pas_p};
	return get_maximum_tuple_size_accomodatable_on_persistent_page(sizeof_ARRAY_TABLE_PAGE_HEADER((&attd)), pas_p->page_size, &((tuple_size_def){.is_variable_sized = 0}));
}

int init_array_table_tuple_definitions(array_table_tuple_defs* attd_p, const page_access_specs* pas_p, const tuple_def* record_def)
{
	// zero initialize attd_p
	(*attd_p) = (array_table_tuple_defs){};

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs for the attd
	attd_p->pas_p = pas_p;

	// this can only be done after setting the pas_p attribute of attd
	// there must be room for atleast some bytes after the array_table_page_header
	if(!can_page_header_fit_on_persistent_page(sizeof_ARRAY_TABLE_PAGE_HEADER(attd_p), attd_p->pas_p->page_size))
		return 0;

	// if the record_def is not fixed sized, then fail
	if(!is_fixed_sized_tuple_def(record_def))
		return 0;

	// initialize index_def
	attd_p->index_def = &(pas_p->page_id_tuple_def);

	// copy record_def
	attd_p->record_def = record_def;

	// number of entries that can fit on the leaf page
	attd_p->leaf_entries_per_page = get_maximum_tuple_count_on_persistent_page(sizeof_ARRAY_TABLE_PAGE_HEADER(attd_p), attd_p->pas_p->page_size, &(attd_p->record_def->size_def));

	// there has to be atleast 1 entries per page for leaf pages
	if(attd_p->leaf_entries_per_page < 1)
	{
		deinit_array_table_tuple_definitions(attd_p);
		return 0;
	}

	// number of entries that can fit on the index (interior) page
	attd_p->index_entries_per_page = get_maximum_tuple_count_on_persistent_page(sizeof_ARRAY_TABLE_PAGE_HEADER(attd_p), attd_p->pas_p->page_size, &(attd_p->index_def->size_def));

	// there has to be atleast 2 entries per page for index_pages, for it to be a tree
	if(attd_p->index_entries_per_page < 2)
	{
		deinit_array_table_tuple_definitions(attd_p);
		return 0;
	}

	// build power_table
	initialize_power_table(&(attd_p->power_table_for_index_entries_per_page), attd_p->index_entries_per_page);

	// calculations for max_array_table_height
	// above attributes must be set successfully for this block of code to run properly
	{
		uint64_t l = 0;
		uint64_t h = UINT64_MAX;
		while(l <= h)
		{
			uint64_t m = l + ((h - l) / 2);
			uint64_t power_result;
			if(!get_leaf_entries_refrenceable_by_entry_at_given_level_using_array_table_tuple_definitions(attd_p, m, &power_result))
			{
				attd_p->max_array_table_height = m;
				h = m - 1;
			}
			else
				l = m + 1;
		}
	}

	return 1;
}

int get_leaf_entries_refrenceable_by_entry_at_given_level_using_array_table_tuple_definitions(const array_table_tuple_defs* attd_p, uint64_t level, uint64_t* result)
{
	// every leaf page entry references only 1 leaf entry, which is itself
	if(level == 0)
	{
		(*result) = 1;
		return 1;
	}

	// result = index_entries_per_page ^ (level - 1)
	// fail if power table fails, i.e. overflows
	if(!get_power_using_power_table(&(attd_p->power_table_for_index_entries_per_page), level - 1, result))
		return 0;

	// check if result * leaf_entries_per_page, will overflow, and then multiply it
	if(will_unsigned_mul_overflow(uint64_t, (*result), attd_p->leaf_entries_per_page))
		return 0;
	(*result) *= attd_p->leaf_entries_per_page;

	return 1;
}

void deinit_array_table_tuple_definitions(array_table_tuple_defs* attd_p)
{
	attd_p->pas_p = NULL;
	attd_p->record_def = NULL;
	attd_p->index_def = NULL;
	attd_p->leaf_entries_per_page = 0;
	attd_p->index_entries_per_page = 0;
}

void print_array_table_tuple_definitions(const array_table_tuple_defs* attd_p)
{
	printf("Array_table tuple defs:\n");

	if(attd_p->pas_p)
		print_page_access_specs(attd_p->pas_p);

	printf("record_def = ");
	if(attd_p->record_def)
		print_tuple_def(attd_p->record_def);
	else
		printf("NULL\n");

	printf("index_def = ");
	if(attd_p->index_def)
		print_tuple_def(attd_p->index_def);
	else
		printf("NULL\n");

	printf("leaf_entries_per_page = %"PRIu64"\n", attd_p->leaf_entries_per_page);
	printf("index_entries_per_page = %"PRIu64"\n", attd_p->index_entries_per_page);

	printf("max_array_table_height = %"PRIu64"\n", attd_p->max_array_table_height);
}