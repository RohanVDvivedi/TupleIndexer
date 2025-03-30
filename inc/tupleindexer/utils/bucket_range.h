#ifndef BUCKET_RANGE_H
#define BUCKET_RANGE_H

#include<stdint.h>

/*
	used for indexing in array_table, page_table, hash_table and the runs inside sorter
*/

// this is an inclusive range of the bucket_id on values in the array_table, page_table and hash_table (uses page_table), even the sorter uses page_table internally
// from first_bucket_id to last_bucket_id both inclusive

typedef struct bucket_range bucket_range;
struct bucket_range
{
	uint64_t first_bucket_id;
	uint64_t last_bucket_id;
};

// bucket_range for a singular bucket
#define BUCKET_RANGE_FOR_BUCKET(bucket_id_val) ((bucket_range){.first_bucket_id = bucket_id_val, .last_bucket_id = bucket_id_val})

// 64 bit range of all buckets in the page table
#define WHOLE_BUCKET_RANGE ((bucket_range){.first_bucket_id = 0, .last_bucket_id = UINT64_MAX})

// returns 1 if br_p->first_bucket_id <= br_p->last_bucket_id, else 0
int is_valid_bucket_range(const bucket_range* br_p);

// obvious
void print_bucket_range(const bucket_range* br_p);

// all the below functions assume that the br's provided as parameters pass valid as per the above function's specifications

// returns 1, if the br1 and br2 are disjoint, i.e. no overlap
int are_disjoint_bucket_range(const bucket_range* br1_p, const bucket_range* br2_p);

// returns !are_disjoint_bucket_range(br1, br2)
int have_overlap_bucket_range(const bucket_range* br1_p, const bucket_range* br2_p);

// returns true if the containee's range is completely contaned in the container's range
int is_contained_bucket_range(const bucket_range* container_p, const bucket_range* containee_p);

// returns true if the containee bucket range is completely contaned in the container's range
int is_bucket_contained_bucket_range(const bucket_range* container_p, uint64_t containee);

#endif