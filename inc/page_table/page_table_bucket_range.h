#ifndef PAGE_TABLE_BUCKET_RANGE_H
#define PAGE_TABLE_BUCKET_RANGE_H

// this is an inclusive range of the bucket_id on values in the page_table
// from first_bucket_id to last_bucket_id both inclusive

typedef struct page_table_bucket_range page_table_bucket_range;
struct page_table_bucket_range
{
	uint64_t first_bucket_id;
	uint64_t last_bucket_id;
};

// returns 1 if ptbr_p->first_bucket_id <= ptbr_p->last_bucket_id, else 0
int is_valid_page_table_bucket_range(const page_table_bucket_range* ptbr_p);

// all the below functions assume that the ptbr's provided as parameters pass valid as per the above function's specifications

// returns 1, if the ptbr1 and ptbr2 are disjoint, i.e. no overlap
int are_disjoint_page_table_bucket_range(const page_table_bucket_range* ptbr1_p, const page_table_bucket_range* ptbr2_p);

// returns !are_disjoint_page_table_bucket_range(ptbr1, ptbr2)
int have_overlap_page_table_bucket_range(const page_table_bucket_range* ptbr1_p, const page_table_bucket_range* ptbr2_p);

// returns true if the containee's range is completely contaned in the container's range
int is_contained_page_table_bucket_range(const page_table_bucket_range* container_p, const page_table_bucket_range* containee_p);

#endif