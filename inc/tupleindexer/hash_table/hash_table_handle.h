#ifndef HASH_TABLE_HANDLE_H
#define HASH_TABLE_HANDLE_H

typedef struct hash_table_handle hash_table_handle;
struct hash_table_handle
{
	uint64_t root_page_id;
	uint64_t bucket_count; // set this to 0, if unknown
};

#endif