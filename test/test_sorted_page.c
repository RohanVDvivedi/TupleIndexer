#include<sorted_packed_page_util.h>

#include<stdio.h>
#include<stdlib.h>
#include<string.h>

#include<alloca.h>

#include<tuple.h>
#include<page_layout_unaltered.h>

#include<unWALed_page_modification_methods.h>

#define PAGE_SIZE 1024

char page[PAGE_SIZE] = {};

tuple_def* get_tuple_definition()
{
	// initialize tuple definition and insert element definitions
	tuple_def* def = get_new_tuple_def("my_table", 3, PAGE_SIZE);

	int res = 0;

	res = insert_element_def(def, "key_1", INT, 4, 0, NULL);
	if(!res)
	{
		printf("failed to insert element def key_1\n");
		exit(-1);
	}
	res = insert_element_def(def, "key_2", VAR_STRING, 1, 0, NULL);
	if(!res)
	{
		printf("failed to insert element def key_2\n");
		exit(-1);
	}
	res = insert_element_def(def, "val", VAR_STRING, 1, 0, NULL);
	if(!res)
	{
		printf("failed to insert element def val\n");
		exit(-1);
	}

	res = finalize_tuple_def(def);
	if(!res)
	{
		printf("failed to finalize tuple def");
		exit(-1);
	}

	if(is_empty_tuple_def(def))
	{
		printf("ERROR BUILDING TUPLE DEFINITION\n");
		exit(-1);
	}

	print_tuple_def(def);
	printf("\n\n");

	return def;
}

// a row like struct for ease in building test tuples
typedef struct row row;
struct row
{
	int32_t key1;
	char* key2;
	char* val;
};

void build_tuple_from_row_struct(const tuple_def* def, void* tuple, const row* r)
{
	init_tuple(def, tuple);

	int column_no = 0;

	set_element_in_tuple(def, column_no++, tuple, &((user_value){.int_value = r->key1}));
	set_element_in_tuple(def, column_no++, tuple, &((user_value){.data = r->key2, .data_size = strlen(r->key2)}));
	set_element_in_tuple(def, column_no++, tuple, &((user_value){.data = r->val, .data_size = strlen(r->val)}));

	printf("Built tuple : size(%u)\n\t", get_tuple_size(def, tuple));
	print_tuple(tuple, def);
	printf("\n\n");
}

int main()
{
	// intialize tuple definition
	tuple_def* def = get_tuple_definition();

	// initialize page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// ---------------	DECLARE TEMP variables

	// to build intermediate tuples (only 1 at a time)
	char tuple_cache[PAGE_SIZE];
	// and
	row* r = NULL;
	// and the result from operation
	int res;

	// ---------------  INITIALIZE PAGE

	if(!pmm_p->init_page(pmm_p->context, (persistent_page){.page = page}, PAGE_SIZE, 0, &(def->size_def)))
	{
		printf("ERROR INITIALIZING THE PAGE\n");
		exit(-1);
	}

	// ---------------	INSERT

	r = &(row){5, "Rohan", "Son of Vipul"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_at_in_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, 0, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){5, "Zohan", "YOU DONT MESS WITH ZOHAN"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_at_in_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, 1, pmm_p);
	printf("Insert_at : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashree", "Daughter of Rupa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashree", "Wife of Manan"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashree", "Daughter of Vipul"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){5, "Rohan", "Son of Rupa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){17, "Adam", "First man"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){10, "Eve", "First woman"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){10, "Eve", "exclaimed Eeeewwwweeehhh"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){17, "Adam", "exclaimed OOOOOOooooohhhhhh"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){5, "Rohan", "created this project"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){2, "Devashre", "sister of Rohan"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){6, "Rupa", "wife of Vipul"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){23, "Jumbo", "not just a squirrel"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){11, "Vipul", "husband of Rupa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){6, "Rupa", "can also be called Roopa"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){14, "Sai", "not just a tortoise"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){14, "Sai", "species: Indian Star Tortoise"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){11, "Vipul", "can also be called Milan"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){23, "Jumbo", "eats dried nuts"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){14, "Sai", "eats green veggies"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){11, "Vipul", "has adam's apple"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){20, "Mithu", "means sweet and can be anyone's nick name"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, NULL, pmm_p);
	printf("Insert : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ----------------  TEST ALL FIND FUNCTIONS

	// intialize single integer key definition
	tuple_def* key_def = get_new_tuple_def("my_table", 1, PAGE_SIZE);
	res = insert_element_def(key_def, "key_1", INT, 4, 0, ZERO_USER_VALUE);
	if(!res)
	{
		printf("failed to insert element def key_1\n");
		exit(-1);
	}
	res = finalize_tuple_def(key_def);
	if(!res)
	{
		printf("failed to finalize tuple def");
		exit(-1);
	}

	for(int32_t i = 0; i <= 25; i++)
	{

		printf("---------------------------------------------------\n\n");
		printf("finding %d -> \n\n", i);

		init_tuple(key_def, tuple_cache);
		set_element_in_tuple(key_def, 0, tuple_cache, &((user_value){.int_value = i}));

		uint32_t index;

		index = find_first_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, tuple_cache, key_def, NULL);
		if(index == NO_TUPLE_FOUND)
			printf("FIND FIRST = %s\n", "NO_TUPLE_FOUND");
		else
			printf("FIND FIRST = %u\n", index);

		index = find_last_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, tuple_cache, key_def, NULL);
		if(index == NO_TUPLE_FOUND)
			printf("FIND LAST = %s\n", "NO_TUPLE_FOUND");
		else
			printf("FIND LAST = %u\n", index);

		printf("\n");

		index = find_preceding_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, tuple_cache, key_def, NULL);
		if(index == NO_TUPLE_FOUND)
			printf("PRECEDING = %s\n", "NO_TUPLE_FOUND");
		else
			printf("PRECEDING = %u\n", index);

		index = find_preceding_equals_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, tuple_cache, key_def, NULL);
		if(index == NO_TUPLE_FOUND)
			printf("PRECEDING EQUALS = %s\n", "NO_TUPLE_FOUND");
		else
			printf("PRECEDING EQUALS = %u\n", index);

		printf("\n");

		index = find_succeeding_equals_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, tuple_cache, key_def, NULL);
		if(index == NO_TUPLE_FOUND)
			printf("SUCCEEDING EQUALS = %s\n", "NO_TUPLE_FOUND");
		else
			printf("SUCCEEDING EQUALS = %u\n", index);

		index = find_succeeding_in_sorted_packed_page(page, PAGE_SIZE, def, NULL, 1, tuple_cache, key_def, NULL);
		if(index == NO_TUPLE_FOUND)
			printf("SUCCEEDING = %s\n", "NO_TUPLE_FOUND");
		else
			printf("SUCCEEDING = %u\n", index);


		printf("\n\n");
	}

	delete_tuple_def(key_def);

	// ----------------  REVERSE PAGE SORT

	reverse_sort_order_on_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, pmm_p);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ----------------  CONVERT IT BACK TO SORTED PACKED PAGE

	sort_and_convert_to_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, pmm_p);
	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ---------------	INSERT

	r = &(row){16, "insert_at", "failed 1"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_at_in_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, 20, pmm_p);
	printf("Insert_at : %d\n\n\n", res);

	// ---------------	INSERT

	r = &(row){21, "insert_at", "failed 2"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_at_in_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, 20, pmm_p);
	printf("Insert_at : %d\n\n\n", res);

	// ---------------	INSERT

	r = &(row){20, "Ainsert_at", "passed"};
	build_tuple_from_row_struct(def, tuple_cache, r);
	res = insert_at_in_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, NULL, 2, tuple_cache, 20, pmm_p);
	printf("Insert_at : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// ----------------  DELETE SOME TUPLES

	res = delete_all_in_sorted_packed_page((persistent_page){.page = page}, PAGE_SIZE, def, 18, 20, pmm_p);
	printf("Delete 18 to 20 : %d\n\n\n", res);

	// ----------------  PRINT PAGE

	print_page(page, PAGE_SIZE, def);
	printf("\n\n");

	// delete helper resources

	delete_tuple_def(def);

	delete_unWALed_page_modification_methods(pmm_p);
	
	return 0;
}