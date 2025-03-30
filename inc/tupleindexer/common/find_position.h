#ifndef FIND_POSITION_H
#define FIND_POSITION_H

typedef enum find_position find_position;
enum find_position
{
	MIN,
	LESSER_THAN,
	LESSER_THAN_EQUALS,
	GREATER_THAN_EQUALS,
	GREATER_THAN,
	MAX,
};

// this macro can be passed to key_element_count_concerned (parameters of bplus_tree or data structures alike), to consider all the key_elements as found in bpttd_p(->key_element_count)
#define KEY_ELEMENT_COUNT UINT32_C(-1)

#endif