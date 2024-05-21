#include<page_table_tuple_definitions.h>

#include<page_table_page_header.h>

#include<persistent_page_functions.h>

#include<cutlery_math.h>

#include<stdlib.h>

int init_page_table_tuple_definitions(page_table_tuple_defs* pttd_p, const page_access_specs* pas_p)
{
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

	// build power_table and optimally set the power_table_overflows_at to the index at which values of power_table starts overflowing
	pttd_p->power_table[0] = pttd_p->entries_per_page;
	for(pttd_p->power_table_overflows_at = 1; pttd_p->power_table_overflows_at < sizeof(pttd_p->power_table)/sizeof(pttd_p->power_table[0]); pttd_p->power_table_overflows_at++)
	{
		if(will_unsigned_mul_overflow(uint64_t, pttd_p->power_table[pttd_p->power_table_overflows_at-1], pttd_p->power_table[pttd_p->power_table_overflows_at-1]))
			break;
		pttd_p->power_table[pttd_p->power_table_overflows_at] = pttd_p->power_table[pttd_p->power_table_overflows_at-1] * pttd_p->power_table[pttd_p->power_table_overflows_at-1];
	}

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

int get_power_of_entries_per_page_using_page_table_tuple_definitions(const page_table_tuple_defs* pttd_p, uint64_t exp, uint64_t* result)
{
	*result = 1;

	// This is a very use of the power_table for entries_per_page ^ exp
	// break exp into its binary representation and product all components
	for(uint8_t bit_index = 0; bit_index < (sizeof(uint64_t) * CHAR_BIT) && ((exp >> bit_index) != UINT64_C(0)); bit_index++)
	{
		// if the number required more or equal bits than the power_table_overflows_at, then fail causing overflow
		if(bit_index >= pttd_p->power_table_overflows_at)
			return 0;

		// get the bit_value of exp at bit_index
		int bit_value = (exp >> bit_index) & 1;
		if(bit_value)
		{
			if(will_unsigned_mul_overflow(uint64_t, pttd_p->power_table[bit_index], (*result)))
				return 0;
			(*result) *= pttd_p->power_table[bit_index];
		}
	}

	// success, no overflows
	return 1;
}

void deinit_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{
	if(pttd_p->entry_def)
		delete_tuple_def(pttd_p->entry_def);

	pttd_p->pas_p = NULL;
	pttd_p->entry_def = NULL;
	pttd_p->entries_per_page = 0;
}

void print_page_table_tuple_definitions(page_table_tuple_defs* pttd_p)
{
	printf("Page_table tuple defs:\n");

	if(pttd_p->pas_p)
		print_page_access_specs(pttd_p->pas_p);

	printf("entry_def = ");
	if(pttd_p->entry_def)
		print_tuple_def(pttd_p->entry_def);
	else
		printf("NULL\n");

	printf("entries_per_page = %"PRIu64"\n", pttd_p->entries_per_page);

	printf("max_page_table_height = %"PRIu64"\n", pttd_p->max_page_table_height);
}