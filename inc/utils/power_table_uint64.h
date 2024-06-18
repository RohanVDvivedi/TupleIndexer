#ifndef POWER_TABLE_UINT64_H
#define POWER_TABLE_UINT64_H

#include<stdint.h>

/*
	This library provides power_table for uint64_t-s, to efficiently and quickly calculate power of a predetermined number
	This number must not be 0 or 1
*/

typedef struct power_table_uint64 power_table_uint64;
struct power_table_uint64
{
	uint64_t power_table[6];

	/*
		power_table[i] = power_table[i-1] ^ 2 = power_table[0] ^ (2 ^ i), for i > 0
		this array is capped to size of 6, allowing only to be indexed by integers from 0 to 5, both inclusive
		this is because the minimum value of power_table[0] is 2, and 2 ^ (2^i) overflows at 6
		so we would never be indexing power_table at index more than or equal to 6
	*/

	uint8_t overflows_at;

	/*
		This is the actual index at which the power_table will overflow
		i.e. for 2 it is 6
	*/
};

// initialize power_table for base = N, i.e. power_table[0] = N
// here N must not equal 0 or 1
int initialize_power_table_uint64(power_table_uint64* ptu_p, uint64_t N);

// returns 1 on success, else will return 0 for an overflow
// on success (return value = 1), then result will be set to ptu_p->power_table[0] ^ exponent
int get_power_using_power_table_uint64(const power_table_uint64* ptu_p, uint64_t exponent, uint64_t* result);

#endif