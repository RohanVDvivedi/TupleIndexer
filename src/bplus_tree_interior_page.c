#include<bplus_tree_interior_page.h>

#include<page_layout.h>

typedef struct bplus_tree_interior_page_header1 bplus_tree_interior_page_header1;
struct bplus_tree_interior_page_header1
{
	uint32_t least_keys_page_id;	// link to child page having keys lesser than the least key on this page
};

typedef struct bplus_tree_interior_page_header2 bplus_tree_interior_page_header2;
struct bplus_tree_interior_page_header2
{
	uint32_t least_keys_page_id;	// link to child page having keys lesser than the least key on this page
};

typedef struct bplus_tree_interior_page_header4 bplus_tree_interior_page_header4;
struct bplus_tree_interior_page_header4
{
	uint32_t least_keys_page_id;	// link to child page having keys lesser than the least key on this page
};

typedef struct bplus_tree_interior_page_header8 bplus_tree_interior_page_header8;
struct bplus_tree_interior_page_header8
{
	uint32_t least_keys_page_id;	// link to child page having keys lesser than the least key on this page
};

uint32_t sizeof_INTERIOR_PAGE_HEADER(bplus_tree_tuple_defs* bpttd_p)
{
	switch(bpttd_p->page_id_width)
	{
		case 1:
			return sizeof(page_header) + sizeof(bplus_tree_page_header) + sizeof(bplus_tree_interior_page_header1);
		case 2:
			return sizeof(page_header) + sizeof(bplus_tree_page_header) + sizeof(bplus_tree_interior_page_header2);
		case 4:
			return sizeof(page_header) + sizeof(bplus_tree_page_header) + sizeof(bplus_tree_interior_page_header4);
		case 8:
			return sizeof(page_header) + sizeof(bplus_tree_page_header) + sizeof(bplus_tree_interior_page_header8);
	}
	return 0;
}

uint64_t get_least_keys_page_id_of_bplus_tree_interior_page(const void* page, bplus_tree_tuple_defs* bpttd_p)
{
	const void* lph = get_page_header((void*)page, bpttd_p->page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	switch(bpttd_p->page_id_width)
	{
		case 1:
			return ((const bplus_tree_interior_page_header1*)lph)->least_keys_page_id;
		case 2:
			return ((const bplus_tree_interior_page_header2*)lph)->least_keys_page_id;
		case 4:
			return ((const bplus_tree_interior_page_header4*)lph)->least_keys_page_id;
		case 8:
			return ((const bplus_tree_interior_page_header8*)lph)->least_keys_page_id;
	}
	return 0;
}

void set_least_keys_page_id_of_bplus_tree_interior_page(void* page, uint64_t page_id, bplus_tree_tuple_defs* bpttd_p)
{
	void* lph = get_page_header(page, bpttd_p->page_size) + sizeof(page_header) + sizeof(bplus_tree_page_header);
	switch(bpttd_p->page_id_width)
	{
		case 1:
		{
			((bplus_tree_interior_page_header1*)lph)->least_keys_page_id = page_id;
			break;
		}
		case 2:
		{
			((bplus_tree_interior_page_header2*)lph)->least_keys_page_id = page_id;
			break;
		}
		case 4:
		{
			((bplus_tree_interior_page_header4*)lph)->least_keys_page_id = page_id;
			break;
		}
		case 8:
		{
			((bplus_tree_interior_page_header8*)lph)->least_keys_page_id = page_id;
			break;
		}
	}
}