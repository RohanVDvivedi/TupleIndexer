#ifndef HASH_TABLE_H
#define HASH_TABLE_H

typedef enum hash_table_type hash_table_type;
enum hash_table_type
{
	BUCKETS_AS_PAGE_LISTS,
	BUCKETS_AS_BPLUS_TREES,
};

#endif