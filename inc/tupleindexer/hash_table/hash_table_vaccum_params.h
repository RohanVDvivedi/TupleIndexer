#ifndef HASH_TABLE_VACCUM_PARAMS_H
#define HASH_TABLE_VACCUM_PARAMS_H

typedef struct hash_table_vaccum_params hash_table_vaccum_params;
struct hash_table_vaccum_params
{
	// low level page table vaccum
	int page_table_vaccum_needed;
	uint64_t page_table_vaccum_bucket_id;

	// high level hash table vaccum
	int hash_table_vaccum_needed;
	const void* hash_table_vaccum_key;
};

#endif