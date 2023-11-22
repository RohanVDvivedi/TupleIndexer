#include<page_table_bucket_range.h>

int is_valid_page_table_bucket_range(const page_table_bucket_range* ptbr_p);

int are_disjoint_page_table_bucket_range(const page_table_bucket_range* ptbr1_p, const page_table_bucket_range* ptbr2_p);

int have_overlap_page_table_bucket_range(const page_table_bucket_range* ptbr1_p, const page_table_bucket_range* ptbr2_p);

int is_contained_page_table_bucket_range(const page_table_bucket_range* container_p, const page_table_bucket_range* containee_p);