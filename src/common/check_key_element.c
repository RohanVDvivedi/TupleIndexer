#include<tupleindexer/common/check_key_element.h>

#include<cutlery/cutlery_math.h>

int is_element_part_of_key(uint32_t key_element_count, const positional_accessor* key_element_ids, const positional_accessor* element_index)
{
	for(uint32_t i = 0; i < key_element_count; i++) // iterate over all the elements one by one
	{
		int match = 1;
		for(uint32_t j = 0; j < min(key_element_ids[i].positions_length, element_index->positions_length); j++) // loop for the common length of the positional_accessor
		{
			if(key_element_ids[i].positions[j] != element_index->positions[j]) // if their any position does not match, then they do not overlap, so make match = 0 and break
			{
				match = 0;
				break;
			}
		}

		// ith key matched so return 1
		// this implies, element_index contains key OR ith portion of the key contains element_index
		if(match)
			return 1;
	}

	// none of the key_element_ids matched so fail
	return 0;
}