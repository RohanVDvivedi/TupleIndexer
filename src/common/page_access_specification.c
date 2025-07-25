#include<tupleindexer/common/page_access_specification.h>

#include<tupleindexer/utils/persistent_page_functions.h>

int initialize_page_access_specs(page_access_specs* pas_p, uint32_t page_id_width, uint32_t page_size, uint64_t NULL_PAGE_ID)
{
	// bytes required to store page id, must be between 1 and 8 both inclusive
	if(page_id_width == 0 || page_id_width > sizeof(uint64_t))
		return 0;

	// NULL_PAGE_ID must fit in page_id_width number of bytes
	if(page_id_width < sizeof(uint64_t) && NULL_PAGE_ID >= ( ((uint64_t)(1)) << (page_id_width * CHAR_BIT) ) )
		return 0;

	pas_p->page_id_width = page_id_width;
	pas_p->page_size = page_size;
	pas_p->NULL_PAGE_ID = NULL_PAGE_ID;

	pas_p->page_id_type_info = define_uint_non_nullable_type("page_id", pas_p->page_id_width);
	if(!initialize_tuple_def(&(pas_p->page_id_tuple_def), &(pas_p->page_id_type_info)))
		return 0;

	pas_p->page_offset_type_info = define_uint_non_nullable_type("page_offset/tuple_index", get_value_size_on_page(page_size));
	if(!initialize_tuple_def(&(pas_p->page_offset_tuple_def), &(pas_p->page_offset_type_info)))
		return 0;

	return 1;
}

int is_valid_page_access_specs(const page_access_specs* pas_p)
{
	// bytes required to store page id, must be between 1 and 8 both inclusive
	if(pas_p->page_id_width == 0 || pas_p->page_id_width > sizeof(uint64_t))
		return 0;

	// NULL_PAGE_ID must fit in page_id_width number of bytes
	if(pas_p->page_id_width < sizeof(uint64_t) && pas_p->NULL_PAGE_ID >= ( ((uint64_t)(1)) << (pas_p->page_id_width * CHAR_BIT) ) )
		return 0;

	if(pas_p->page_id_tuple_def.type_info == NULL || pas_p->page_id_tuple_def.type_info != &(pas_p->page_id_type_info) || pas_p->page_id_type_info.is_finalized == 0)
		return 0;

	if(pas_p->page_offset_tuple_def.type_info == NULL || pas_p->page_offset_tuple_def.type_info != &(pas_p->page_offset_type_info) || pas_p->page_offset_type_info.is_finalized == 0)
		return 0;

	return 1;
}

void print_page_access_specs(const page_access_specs* pas_p)
{
	printf("page_id_width = %"PRIu8"\n", pas_p->page_id_width);
	printf("page_size = %"PRIu32"\n", pas_p->page_size);
	printf("NULL_PAGE_ID = %"PRIu64"\n", pas_p->NULL_PAGE_ID);
	printf("page_id_type_info = \n");
	print_tuple_def(&(pas_p->page_id_tuple_def));
	printf("page_offset_type_info = \n");
	print_tuple_def(&(pas_p->page_offset_tuple_def));
	printf("\n");
}