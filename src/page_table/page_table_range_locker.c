#include<page_table_range_locker.h>

page_table_range_locker* get_new_page_table_range_locker(uint64_t root_page_id, page_table_bucket_range lock_range, int lock_type, const page_table_tuple_defs* pttd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

int minimize_lock_range_for_page_table_range_locker(page_table_range_locker* ptrl_p, page_table_bucket_range lock_range, const void* transaction_id, int* abort_error);

page_table_bucket_range get_lock_range_for_page_table_range_locker(const page_table_range_locker* ptrl_p);

int is_writable_page_table_range_locker(const page_table_range_locker* ptrl_p);

uint64_t get_from_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, const void* transaction_id, int* abort_error);

int set_in_page_table(page_table_range_locker* ptrl_p, uint64_t bucket_id, uint64_t page_id, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

void delete_page_table_range_locker(page_table_range_locker* ptrl_p, const void* transaction_id, int* abort_error);