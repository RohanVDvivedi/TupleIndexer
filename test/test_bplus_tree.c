#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>

#include<bplus_tree.h>
#include<bplus_tree_tuple_definitions.h>
#include<data_access_methods.h>
#include<in_memory_data_store.h>

typedef struct row row;
struct row
{
	uint32_t index;
	char name[64];
	uint8_t age;
	char sex_char[8]; // Female = 0 or Male = 1
	char email[64];
	char phone[64];
};

void read_from_file(row* r, int fd);

void write_row_to_tuple(void* tuple, const row* r);

void read_row_from_tuple(row* r, const void* tuple);

void print_row(row* r);

void init_tuple_definition(tuple_def* def);

void build_tuple_from_row_struct(const tuple_def* def, void* tuple, const row* r);

int main()
{
	return 0;
}