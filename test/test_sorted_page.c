#include<sorted_packed_page_util.h>

#include<stdio.h>
#include<stdlib.h>
#include<string.h>

#include<alloca.h>

#include<tuple.h>
#include<page_layout.h>

#define PAGE_SIZE 1024

char page[PAGE_SIZE] = {};

void init_tuple_definition(tuple_def* def)
{
	// initialize tuple definition and insert element definitions
	int res = init_tuple_def(def, "my_table");

	res = insert_element_def(def, "key_1", INT, 4);

	res = insert_element_def(def, "key_2", VAR_STRING, 1);

	res = insert_element_def(def, "val", VAR_STRING, 1);

	finalize_tuple_def(def, PAGE_SIZE);

	if(is_empty_tuple_def(def))
	{
		printf("ERROR BUILDING TUPLE DEFINITION\n");
		exit(-1);
	}

	print_tuple_def(def);
	printf("\n\n");
}

// a row like struct for ease in building test tuples
typedef struct row row;
struct row
{
	uint32_t key1;
	char* key2;
	char* val;
};

void build_tuple_from_row_struct(const tuple_def* def, void* tuple, const row* r)
{
	init_tuple(def, tuple);

	int column_no = 0;

	set_element_in_tuple(def, column_no++, tuple, &(r->key1), -1);
	set_element_in_tuple(def, column_no++, tuple, (r->key2), -1);
	set_element_in_tuple(def, column_no++, tuple, (r->val), -1);

	// output print string
	char print_buffer[PAGE_SIZE];

	sprint_tuple(print_buffer, tuple, def);
	printf("Built tuple : size(%u)\n\t%s\n\n", get_tuple_size(def, tuple), print_buffer);
}

int main()
{
	// allocate size of tuple definition
	tuple_def* def = alloca(size_of_tuple_def(16));

	// initialize tuple definition and insert element definitions
	init_tuple_definition(def);

	// ---------------	DECLARE TEMP variables

	// to build intermediate tuples (only 1 at a time)
	char tuple_cache[PAGE_SIZE];
	// and
	row* r = NULL;
	// and the result from operation
	int res;

	// ---------------  INITIALIZE PAGE

	if(!init_page(page, PAGE_SIZE, 0, def))
	{
		printf("ERROR INITIALIZING THE PAGE\n");
		exit(-1);
	}

	// ---------------	INSERT

	r = &(row){5, "Rohan", "Son of Vipul"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashree", "Daughter of Rupa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashree", "Wife of Manan"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashree", "Daughter of Vipul"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){5, "Rohan", "Son of Rupa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){17, "Adam", "First man"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){10, "Eve", "First woman"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){10, "Eve", "exclaimed Eeeewwwweeehhh"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){17, "Adam", "exclaimed OOOOOOooooohhhhhh"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){5, "Rohan", "created this project"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashre", "sister of Rohan"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){6, "Rupa", "wife of Vipul"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){23, "Jumbo", "not just a squirrel"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){11, "Vipul", "husband of Rupa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){6, "Rupa", "can also be called Roopa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){14, "Sai", "not just a tortoise"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){14, "Sai", "species: Indian Star Tortoise"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){11, "Vipul", "can also be called Milan"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){23, "Jumbo", "eats dried nuts"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){14, "Sai", "eats green veggies"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){11, "Vipul", "has adam's apple"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){20, "Mithu", "means sweet and can be anyone's nick name"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ----------------  TEST ALL FIND FUNCTIONS

	for(int32_t i = 0; i <= 25; i++)
	{

		printf("---------------------------------------------------\n\n");
		printf("finding %d -> \n\n", i);

		uint32_t index;

		index = find_first_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, &i);
		if(index == NOT_FOUND)
			printf("FIND FIRST = %s\n", "NOT_FOUND");
		else
			printf("FIND FIRST = %u\n", index);

		index = find_last_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, &i);
		if(index == NOT_FOUND)
			printf("FIND LAST = %s\n", "NOT_FOUND");
		else
			printf("FIND LAST = %u\n", index);

		printf("\n");

		index = find_preceding_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, &i);
		if(index == NOT_FOUND)
			printf("PRECEDING = %s\n", "NOT_FOUND");
		else
			printf("PRECEDING = %u\n", index);

		index = find_preceding_equals_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, &i);
		if(index == NOT_FOUND)
			printf("PRECEDING EQUALS = %s\n", "NOT_FOUND");
		else
			printf("PRECEDING EQUALS = %u\n", index);

		printf("\n");

		index = find_succeeding_equals_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, &i);
		if(index == NOT_FOUND)
			printf("SUCCEEDING EQUALS = %s\n", "NOT_FOUND");
		else
			printf("SUCCEEDING EQUALS = %u\n", index);

		index = find_succeeding_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, &i);
		if(index == NOT_FOUND)
			printf("SUCCEEDING = %s\n", "NOT_FOUND");
		else
			printf("SUCCEEDING = %u\n", index);


		printf("\n\n");
	}

	// ----------------  REVERSE PAGE SORT

	reverse_sort_order_on_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ----------------  CONVERT IT BACK TO SORTED PACKED PAGE

	sort_and_convert_to_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n");
	
	return 0;
}