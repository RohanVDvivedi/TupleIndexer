#include<tupleindexer/heap_table/heap_table_tuple_definitions.h>

#include<stdlib.h>

#define UNUSED_SPACE_ENTRY_DEF_POSITION 0
#define PAGE_ID_ENTRY_DEF_POSITION      1

positional_accessor entry_key_element_ids[2] = {STATIC_POSITION(UNUSED_SPACE_ENTRY_DEF_POSITION), STATIC_POSITION(PAGE_ID_ENTRY_DEF_POSITION)}; // entry's key is itself, i.e. this must contain {0,1}
compare_direction entry_key_compare_direction[2] = {ASC, ASC}; // entry's key compare directions, both ascending (find page with least unused space first (for best-fit) and the fill the page with lower page_id first)

int init_heap_table_tuple_definitions(heap_table_tuple_defs* httd_p, const page_access_specs* pas_p, const tuple_def* record_def)
{
	// zero initialize httd_p
	(*httd_p) = (heap_table_tuple_defs){};

	// basic parameter check
	if(record_def == NULL)
		return 0;

	// check id page_access_specs struct is valid
	if(!is_valid_page_access_specs(pas_p))
		return 0;

	// initialize page_access_specs fo the bpttd
	httd_p->pas_p = pas_p;

	httd_p->record_def = record_def;

	// initialize entry def

	// allocate memory for key_def and initialize it
	{
		data_type_info* entry_type_info = malloc(sizeof_tuple_data_type_info(2));
		if(entry_type_info == NULL)
			exit(-1);
		initialize_tuple_data_type_info(entry_type_info, "temp_heap_table_entry_def", 1, pas_p->page_size, 2);

		strcpy(entry_type_info->containees[UNUSED_SPACE_ENTRY_DEF_POSITION].field_name, "unused_space");
		entry_type_info->containees[UNUSED_SPACE_ENTRY_DEF_POSITION].al.type_info = (data_type_info*)(&(pas_p->page_offset_type_info));

		strcpy(entry_type_info->containees[PAGE_ID_ENTRY_DEF_POSITION].field_name, "page_id");
		entry_type_info->containees[PAGE_ID_ENTRY_DEF_POSITION].al.type_info = (data_type_info*)(&(pas_p->page_id_type_info));

		httd_p->entry_def = malloc(sizeof(tuple_def));
		if(httd_p->entry_def == NULL)
			exit(-1);
		if(!initialize_tuple_def(httd_p->entry_def, entry_type_info))
		{
			free(httd_p->entry_def);
			free(entry_type_info);
			httd_p->entry_def = NULL; // avoid double free due to below call
			deinit_heap_table_tuple_definitions(httd_p);
			return 0;
		}
	}

	if(!init_bplus_tree_tuple_definitions(&(httd_p->bpttd), pas_p, httd_p->entry_def, entry_key_element_ids, entry_key_compare_direction, 2))
	{
		deinit_heap_table_tuple_definitions(httd_p);
		return 0;
	}

	return 1;
}

void deinit_heap_table_tuple_definitions(heap_table_tuple_defs* httd_p)
{
	if(httd_p->entry_def)
	{
		if(httd_p->entry_def->type_info)
			free(httd_p->entry_def->type_info);
		free(httd_p->entry_def);
	}

	httd_p->pas_p = NULL;
	httd_p->record_def = NULL;
	deinit_bplus_tree_tuple_definitions(&(httd_p->bpttd));
	httd_p->entry_def = NULL;
}

void print_heap_table_tuple_definitions(heap_table_tuple_defs* httd_p)
{
	printf("Heap_table tuple defs:\n");

	if(httd_p->pas_p)
		print_page_access_specs(httd_p->pas_p);

	printf("record_def = ");
	if(httd_p->record_def)
		print_tuple_def(httd_p->record_def);
	else
		printf("NULL\n");

	print_bplus_tree_tuple_definitions(&(httd_p->bpttd));

	printf("entry_def = ");
	if(httd_p->entry_def)
		print_tuple_def(httd_p->entry_def);
	else
		printf("NULL\n");
}

void build_heap_table_entry_tuple(const heap_table_tuple_defs* httd_p, void* entry_tuple, uint32_t unused_space, uint64_t page_id)
{
	init_tuple(httd_p->entry_def, entry_tuple);
	set_element_in_tuple(httd_p->entry_def, STATIC_POSITION(UNUSED_SPACE_ENTRY_DEF_POSITION), entry_tuple, &((user_value){.uint_value = unused_space}), 0);
	set_element_in_tuple(httd_p->entry_def, STATIC_POSITION(PAGE_ID_ENTRY_DEF_POSITION), entry_tuple, &((user_value){.uint_value = page_id}), 0);
}

uint64_t decompose_heap_table_entry_tuple(const heap_table_tuple_defs* httd_p, const void* entry_tuple, uint32_t* unused_space)
{
	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, httd_p->entry_def, STATIC_POSITION(UNUSED_SPACE_ENTRY_DEF_POSITION), entry_tuple);
		(*unused_space) = uval.uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, httd_p->entry_def, STATIC_POSITION(PAGE_ID_ENTRY_DEF_POSITION), entry_tuple);
		return uval.uint_value;
	}
}