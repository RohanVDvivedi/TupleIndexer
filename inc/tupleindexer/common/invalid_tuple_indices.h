#ifndef INVALID_TUPLE_INDICES_H
#define INVALID_TUPLE_INDICES_H

// index of any tuple and the tuple_count of any page in a page is represented in 32 bit unsigned integer
// Thus a tuple will never have an index of UINT32_MAX, so this becomes the invalid tuple index
#define INVALID_TUPLE_INDEX UINT32_MAX

// returned by fiund functions of this library, when the tuple you were looking for could not be found
#define NO_TUPLE_FOUND INVALID_TUPLE_INDEX

#endif