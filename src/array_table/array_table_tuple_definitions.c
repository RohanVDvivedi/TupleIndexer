#include<array_table_tuple_definitions.h>

#include<array_table_page_header.h>

#include<persistent_page_functions.h>

#include<cutlery_math.h>

#include<stdlib.h>

int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p)
{
	// zero initialize pttd_p
	(*pttd_p) = (page_table_tuple_defs){};

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs fo the bpttd
	pttd_p->pas_p = pas_p;

	// this can only be done after setting the pas_p attribute of pttd
	// there must be room for atleast some bytes after the page_table_page_header
	if(!can_page_header_fit_on_persistent_page(sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), pttd_p->pas_p->page_size))
		return 0;

	int res = 1;

	// initialize entry_def
	pttd_p->entry_def = get_new_tuple_def("temp_entry_def", 1, pttd_p->pas_p->page_size);
	if(pttd_p->entry_def == NULL) // memory allocation failed
		exit(-1);
	res = insert_element_def(pttd_p->entry_def, "child_page_id", UINT, pttd_p->pas_p->page_id_width, 1, &((user_value){.uint_value = pttd_p->pas_p->NULL_PAGE_ID}));
	if(res == 0)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}
	res = finalize_tuple_def(pttd_p->entry_def);
	if(res == 0)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}

	// number of entries that can fir on the page
	pttd_p->entries_per_page = get_maximum_tuple_count_on_persistent_page(sizeof_PAGE_TABLE_PAGE_HEADER(pttd_p), pttd_p->pas_p->page_size, &(pttd_p->entry_def->size_def));

	// there has to be atleast 2 entries per page for it to be a tree
	if(pttd_p->entries_per_page < 2)
	{
		deinit_page_table_tuple_definitions(pttd_p);
		return 0;
	}

	// build power_table
	initialize_power_table(&(pttd_p->power_table_for_entries_per_page), pttd_p->entries_per_page);
	

	// calculations for max_page_table_height
	// above attributes must be set successfully for this block of code to run properly
	{
		uint64_t l = 0;
		uint64_t h = UINT64_MAX;
		while(l <= h)
		{
			uint64_t m = l + ((h - l) / 2);
			uint64_t exp_m;
			if(!get_power_of_entries_per_page_using_page_table_tuple_definitions(pttd_p, m, &exp_m))
			{
				pttd_p->max_page_table_height = m;
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
	if(pttd_p->record_def)
		delete_tuple_def(pttd_p->record_def);
	if(pttd_p->index_def)
		delete_tuple_def(pttd_p->index_def);

	pttd_p->pas_p = NULL;
	pttd_p->entry_def = NULL;
	pttd_p->leaf_entries_per_page = 0;
	pttd_p->index_entries_per_page = 0;
}

void print_array_table_tuple_definitions(const array_table_tuple_defs* attd_p)
{
	printf("Array_table tuple defs:\n");

	if(pttd_p->pas_p)
		print_page_access_specs(pttd_p->pas_p);

	printf("record_def = ");
	if(pttd_p->record_def)
		print_tuple_def(pttd_p->record_def);
	else
		printf("NULL\n");

	printf("index_def = ");
	if(pttd_p->index_def)
		print_tuple_def(pttd_p->index_def);
	else
		printf("NULL\n");

	printf("leaf_entries_per_page = %"PRIu64"\n", pttd_p->leaf_entries_per_page);
	printf("index_entries_per_page = %"PRIu64"\n", pttd_p->index_entries_per_page);

	printf("max_page_table_height = %"PRIu64"\n", pttd_p->max_page_table_height);
}