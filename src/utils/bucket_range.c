#include<bucket_range.h>

#include<inttypes.h>
#include<stdio.h>

int is_valid_bucket_range(const bucket_range* br_p)
{
	return br_p->first_bucket_id <= br_p->last_bucket_id;
}

void print_bucket_range(const bucket_range* br_p)
{
	printf("[%"PRIu64", %"PRIu64"]", br_p->first_bucket_id, br_p->last_bucket_id);
}

int are_disjoint_bucket_range(const bucket_range* br1_p, const bucket_range* br2_p)
{
	// it is either range of 1 comes before 2 or when range of 2 comes before 1
	return (br1_p->last_bucket_id < br2_p->first_bucket_id) || (br2_p->last_bucket_id < br1_p->first_bucket_id);
}

int have_overlap_bucket_range(const bucket_range* br1_p, const bucket_range* br2_p)
{
	return !are_disjoint_bucket_range(br1_p, br2_p);
}

int is_contained_bucket_range(const bucket_range* container_p, const bucket_range* containee_p)
{
	return (container_p->first_bucket_id <= containee_p->first_bucket_id) && (containee_p->last_bucket_id <= container_p->last_bucket_id);
}

int is_bucket_contained_bucket_range(const bucket_range* container_p, uint64_t containee)
{
	return (container_p->first_bucket_id <= containee) && (containee <= container_p->last_bucket_id);
}