#include<power_table.h>

#include<cutlery_math.h>

int initialize_power_table(power_table* pt_p, uint64_t N)
{
	// power table for 0 and 1 can not be generated
	if(N == 0 || N == 1)
		return 0;

	// initialize the first value, power_table[0] = N ^ (2 ^ 0) = N
	pt_p->power_table[0] = N;

	for(pt_p->overflows_at = 1; pt_p->overflows_at < sizeof(pt_p->power_table)/sizeof(pt_p->power_table[0]); pt_p->overflows_at++)
	{
		if(will_unsigned_mul_overflow(uint64_t, pt_p->power_table[pt_p->overflows_at-1], pt_p->power_table[pt_p->overflows_at-1]))
			break;

		// power_table[i] = power_table[0] ^ (2 ^ i) = (power_table[0] ^ (2 ^ (i-1))) ^ 2 = power_table[i-1] ^ 2
		pt_p->power_table[pt_p->overflows_at] = pt_p->power_table[pt_p->overflows_at-1] * pt_p->power_table[pt_p->overflows_at-1];
	}

	return 1;
}

int get_power_using_power_table(const power_table* pt_p, uint64_t exponent, uint64_t* result)
{
	(*result) = 1;

	// This is a very use of the power_table for entries_per_page ^ exp
	// break exp into its binary representation and product all components
	for(uint8_t bit_index = 0; bit_index < (sizeof(uint64_t) * CHAR_BIT) && ((exponent >> bit_index) != UINT64_C(0)); bit_index++)
	{
		// if the number required more or equal bits than the power_table_overflows_at, then fail causing overflow
		if(bit_index >= pt_p->overflows_at)
			return 0;

		// get the bit_value of exp at bit_index
		int bit_value = (exponent >> bit_index) & 1;
		if(bit_value)
		{
			// if the product would cause over flow then fail
			if(will_unsigned_mul_overflow(uint64_t, pt_p->power_table[bit_index], (*result)))
				return 0;

			(*result) *= pt_p->power_table[bit_index];
		}
	}

	// success, no overflows
	return 1;
}