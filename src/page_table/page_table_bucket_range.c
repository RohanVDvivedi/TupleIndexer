#include<page_table_bucket_range.h>

int is_valid_page_table_bucket_range(const page_table_bucket_range* ptbr_p)
{
	return ptbr_p->first_bucket_id <= ptbr_p->last_bucket_id;
}

int are_disjoint_page_table_bucket_range(const page_table_bucket_range* ptbr1_p, const page_table_bucket_range* ptbr2_p)
{
	// it is either range of 1 comes before 2 or when range of 2 comes before 1
	return (ptbr1_p->last_bucket_id < ptbr2_p->first_bucket_id) || (ptbr2_p->last_bucket_id < ptbr1_p->first_bucket_id);
}

int have_overlap_page_table_bucket_range(const page_table_bucket_range* ptbr1_p, const page_table_bucket_range* ptbr2_p)
{
	return !are_disjoint_page_table_bucket_range(ptbr1_p, ptbr2_p);
}

int is_contained_page_table_bucket_range(const page_table_bucket_range* container_p, const page_table_bucket_range* containee_p)
{
	return (container_p->first_bucket_id <= containee_p->first_bucket_id) && (containee_p->last_bucket_id <= container_p->last_bucket_id);
}

int is_bucket_contained_page_table_bucket_range(const page_table_bucket_range* container_p, uint64_t containee)
{
	return (container_p->first_bucket_id <= containee) && (containee <= container_p->last_bucket_id);
}