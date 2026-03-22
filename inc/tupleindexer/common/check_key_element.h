#ifndef CHECK_KEY_ELEMENT_H
#define CHECK_KEY_ELEMENT_H

#include<tuplestore/positional_accessor.h>

// there are key_element_count of key_element_ids
int is_element_part_of_key(uint32_t key_element_count, const positional_accessor* key_element_ids, const positional_accessor* element_index);

/*
	ideally every datastructure has a key and value (bplus_tree and hash_table surely does)
	the key is specified using the key_element_ids representing, key_element_count number of attributes of the tuple as key
	now if you modify some attribute of the tuple at positional accessor of element_index, you might need to re-index this tuple
	OR you may not be allowed to modify this attribute of the tuple at-all, if it resides in the index (or this data structure)
	so this is the use case, that this function solves
*/

#endif