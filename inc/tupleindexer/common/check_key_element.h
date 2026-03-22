#ifndef CHECK_KEY_ELEMENT_H
#define CHECK_KEY_ELEMENT_H

#include<tuplestore/positional_accessor.h>

// there are key_element_count of key_element_ids
int is_element_part_of_key(uint32_t key_element_count, const positional_accessor* key_element_ids, const positional_accessor* element_index);

#endif