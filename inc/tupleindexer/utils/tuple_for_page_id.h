#ifndef TUPLE_FOR_PAGE_ID_H
#define TUPLE_FOR_PAGE_ID_H

/*
	A page_id is always stored in uint64_t data type in this library (TupleIndexer)
	and so page_id_width must lie between 1 and 8, both inclusive
	hence a tuple that only contains a non nullable fixed width unsigned page_id will be atmost 8 bytes in size
*/

#define MAX_TUPLE_SIZE_FOR_ONLY_NON_NULLABLE_FIXED_WIDTH_UNSIGNED_PAGE_ID 8

#endif