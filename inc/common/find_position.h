#ifndef FIND_POSITION_H
#define FIND_POSITION_H

// order of the enum values in find_position must remain the same
// bplus_tree's internal enums used for find depend on this.

typedef enum find_position find_position;
enum find_position
{
	LESSER_THAN,
	LESSER_THAN_EQUALS,
	GREATER_THAN_EQUALS,
	GREATER_THAN,
};

#endif