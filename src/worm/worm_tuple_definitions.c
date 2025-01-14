#include<worm_tuple_definitions_public.h>

#include<worm_head_page_header.h>
#include<worm_any_page_header.h>

int init_worm_tuple_definitions(worm_tuple_defs* wtd_p, const page_access_specs* pas_p)
{
	// zero initialize attd_p
	(*wtd_p) = (worm_tuple_defs){};

	// check if page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs for the wtd
	wtd_p->pas_p = pas_p;

	// this can only be done after setting the pas_p attribute of wtd
	// there must be room for atleast some bytes after the worm_head_page_header
	if(!can_page_header_fit_on_persistent_page(sizeof_WORM_HEAD_PAGE_HEADER(wtd_p), wtd_p->pas_p->page_size))
		return 0;

	// this can only be done after setting the pas_p attribute of wtd
	// there must be room for atleast some bytes after the worm_any_page_header
	if(!can_page_header_fit_on_persistent_page(sizeof_WORM_ANY_PAGE_HEADER(wtd_p), wtd_p->pas_p->page_size))
		return 0;

	// create a blob type that could possibly span a complete page
	{
		data_type_info* partial_blob_type_info = malloc(sizeof(data_type_info));
		if(partial_blob_type_info == NULL)
			exit(-1);
		(*partial_blob_type_info) = get_variable_length_blob_type("worm_blob", wtd_p->pas_p->page_size);

		wtd_p->partial_blob_tuple_def = malloc(sizeof(tuple_def));
		if(wtd_p->partial_blob_tuple_def == NULL)
			exit(-1);
		if(!initialize_tuple_def(wtd_p->partial_blob_tuple_def, partial_blob_type_info))
		{
			free(partial_blob_type_info);
			free(wtd_p->partial_blob_tuple_def);
			deinit_worm_tuple_definitions(wtd_p);
			return 0;
		}
	}

	// you must have atleast 1 byte for the contents of the blob, and any additional space overhead due to the page organization
	uint32_t min_tuple_space_requirement = get_minimum_tuple_size(wtd_p->partial_blob_tuple_def) + 1 + get_additional_space_overhead_per_tuple_on_persistent_page(wtd_p->pas_p->page_size, &(wtd_p->partial_blob_tuple_def->size_def));

	// if the head page could not even fit the minimum space requirement of the 1 byte blob, then fail
	if(min_tuple_space_requirement > get_space_to_be_allotted_to_all_tuples_on_persistent_page(sizeof_WORM_HEAD_PAGE_HEADER(wtd_p), wtd_p->pas_p->page_size, &(wtd_p->partial_blob_tuple_def->size_def)))
	{
		deinit_worm_tuple_definitions(wtd_p);
		return 0;
	}

	// if the any page could not even fit the minimum space requirement of the 1 byte blob, then fail
	if(min_tuple_space_requirement > get_space_to_be_allotted_to_all_tuples_on_persistent_page(sizeof_WORM_ANY_PAGE_HEADER(wtd_p), wtd_p->pas_p->page_size, &(wtd_p->partial_blob_tuple_def->size_def)))
	{
		deinit_worm_tuple_definitions(wtd_p);
		return 0;
	}

	return 1;
}

void deinit_worm_tuple_definitions(worm_tuple_defs* wtd_p)
{
	if(wtd_p->partial_blob_tuple_def)
	{
		if(wtd_p->partial_blob_tuple_def->type_info)
			free(wtd_p->partial_blob_tuple_def->type_info);
		free(wtd_p->partial_blob_tuple_def);
	}
	(*wtd_p) = (worm_tuple_defs){};
}

void print_worm_tuple_definitions(const worm_tuple_defs* wtd_p)
{
	printf("Worm tuple defs:\n");

	if(wtd_p->pas_p)
		print_page_access_specs(wtd_p->pas_p);

	printf("partial_blob_tuple_def = ");
	if(wtd_p->partial_blob_tuple_def)
		print_tuple_def(wtd_p->partial_blob_tuple_def);
	else
		printf("NULL\n");
}