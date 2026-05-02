#include<tupleindexer/blob_store/blob_store_tuple_definitions_public.h>

#include<tupleindexer/heap_page/heap_page_header.h>

int init_blob_store_tuple_definitions(blob_store_tuple_defs* bstd_p, const page_access_specs* pas_p)
{
	// zero initialize bstd_p
	(*bstd_p) = (blob_store_tuple_defs){};

	// check if page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs for the bstd
	bstd_p->pas_p = pas_p;

	// this can only be done after setting the pas_p attribute of bstd
	// there must be room for atleast some bytes after the heap_page_header
	if(!can_page_header_fit_on_persistent_page(sizeof_HEAP_PAGE_HEADER(bstd_p->pas_p), bstd_p->pas_p->page_size))
		return 0;

	// create a chunk that could possibly span a complete page
	{
		data_type_info* chunk_data_type_info = malloc(sizeof(data_type_info));
		if(chunk_data_type_info == NULL)
			exit(-1);
		(*chunk_data_type_info) = get_variable_length_binary_type("chunk_data", bstd_p->pas_p->page_size);

		data_type_info* chunk_type_info = malloc(sizeof_tuple_data_type_info(3));
		if(chunk_type_info == NULL)
			exit(-1);
		initialize_tuple_data_type_info(chunk_type_info, "chunk", 0, bstd_p->pas_p->page_size, 3);

		strcpy(chunk_type_info->containees[0].field_name, "chunk_data");
		chunk_type_info->containees[0].al.type_info = chunk_data_type_info;

		strcpy(chunk_type_info->containees[1].field_name, "next_page_id");
		chunk_type_info->containees[1].al.type_info = (data_type_info*)(&(bstd_p->pas_p->page_id_type_info));

		strcpy(chunk_type_info->containees[2].field_name, "next_tuple_index");
		chunk_type_info->containees[2].al.type_info = (data_type_info*)(&(bstd_p->pas_p->tuple_index_type_info));

		bstd_p->chunk_tuple_def = malloc(sizeof(tuple_def));
		if(bstd_p->chunk_tuple_def == NULL)
			exit(-1);
		if(!initialize_tuple_def(bstd_p->chunk_tuple_def, chunk_type_info))
		{
			free(chunk_data_type_info);
			free(chunk_type_info);
			free(bstd_p->chunk_tuple_def);
			bstd_p->chunk_tuple_def = NULL; // avoid double free due to below call
			deinit_blob_store_tuple_definitions(bstd_p);
			return 0;
		}
	}

	// compute min_chunk_size and max_chunk_size

	uint32_t additional_bytes_for_slot_per_chunk = get_additional_space_overhead_per_tuple_on_persistent_page(bstd_p->pas_p->page_size, &(bstd_p->chunk_tuple_def->size_def));

	// minimum size of the chunk_tuple_def + sizeof of 1 byte chunk
	bstd_p->min_chunk_size = get_minimum_tuple_size(bstd_p->chunk_tuple_def) + bstd_p->chunk_tuple_def->type_info->containees[0].al.type_info->min_size + 1 + additional_bytes_for_slot_per_chunk;

	bstd_p->max_chunk_size = get_maximum_tuple_size_accomodatable_on_persistent_page(sizeof_HEAP_PAGE_HEADER(bstd_p->pas_p), bstd_p->pas_p->page_size, &(bstd_p->chunk_tuple_def->size_def)) + additional_bytes_for_slot_per_chunk;

	// minimal check
	if(bstd_p->max_chunk_size < bstd_p->min_chunk_size)
	{
		deinit_blob_store_tuple_definitions(bstd_p);
		return 0;
	}

	bstd_p->max_data_bytes_in_chunk = bstd_p->max_chunk_size - bstd_p->min_chunk_size + 1;

	if(!init_heap_table_tuple_definitions(&(bstd_p->httd), bstd_p->pas_p, bstd_p->chunk_tuple_def))
	{
		deinit_blob_store_tuple_definitions(bstd_p);
		return 0;
	}

	return 1;
}

void deinit_blob_store_tuple_definitions(blob_store_tuple_defs* bstd_p)
{
	deinit_heap_table_tuple_definitions(&(bstd_p->httd));

	if(bstd_p->chunk_tuple_def)
	{
		if(bstd_p->chunk_tuple_def->type_info)
		{
			if(bstd_p->chunk_tuple_def->type_info->containees[0].al.type_info)
				free(bstd_p->chunk_tuple_def->type_info->containees[0].al.type_info);
			free(bstd_p->chunk_tuple_def->type_info);
		}
		free(bstd_p->chunk_tuple_def);
	}

	(*bstd_p) = (blob_store_tuple_defs){};
}

void print_blob_store_tuple_definitions(const blob_store_tuple_defs* bstd_p)
{
	printf("Blob store tuple defs:\n");

	if(bstd_p->pas_p)
		print_page_access_specs(bstd_p->pas_p);

	printf("chunk_tuple_def = ");
	if(bstd_p->chunk_tuple_def)
		print_tuple_def(bstd_p->chunk_tuple_def);
	else
		printf("NULL\n");

	printf("min_chunk_size = %"PRIu32"\n", bstd_p->min_chunk_size);
	printf("max_chunk_size = %"PRIu32"\n", bstd_p->max_chunk_size);
	printf("max_data_bytes_in_chunk = %"PRIu32"\n", bstd_p->max_data_bytes_in_chunk);

	print_heap_table_tuple_definitions(&(bstd_p->httd));
}