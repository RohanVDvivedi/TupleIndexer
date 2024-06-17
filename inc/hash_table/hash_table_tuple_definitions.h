#ifndef HASH_TABLE_TUPLE_DEFINITIONS_H
#define HASH_TABLE_TUPLE_DEFINITIONS_H

#include<hash_table_tuple_definitions_public.h>

// returns floor(log(x)/log(2)) = floor(log(x) base 2)
// uses binary searchover sample space of 0 to 63
// any other number is an error from this function
int64_t get_floor_log_base_2(uint64_t x);

// returns = bucket_count % (2 ^ get_floor_log_base_2(bucket_count))
// this is the bucket_index that needs to be split for inscreasing bucket_count while linear hashing
// this function also returns the floor(log(bucket_count) bas 2), in the floor_log_2 variable
uint64_t get_hash_table_split_index(uint64_t bucket_count, int64_t* floor_log_2);

#endif