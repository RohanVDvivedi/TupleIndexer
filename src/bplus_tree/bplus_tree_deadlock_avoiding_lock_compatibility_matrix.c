#include<tupleindexer/bplus_tree/bplus_tree_deadlock_avoiding_lock_compatibility_matrix.h>

#define MODES_COUNT 6

/*
	Here the comments represent
	PO  -> BPLUS_TREE_POINT_OPERATION
	LFR -> BPLUS_TREE_LEAF_ONLY_FORWARD_READ_ITERATOR
	LBR -> BPLUS_TREE_LEAF_ONLY_BACKWARD_READ_ITERATOR
	LFW -> BPLUS_TREE_LEAF_ONLY_FORWARD_WRITE_ITERATOR
	LBW -> BPLUS_TREE_LEAF_ONLY_BACKWARD_WRITE_ITERATOR
	SO  -> BPLUS_TREE_STACKED_ITERATOR_OPERATIONS
*/

const glock_matrix bplus_tree_deadlock_avoidance_lock_matrix = {
	.lock_modes_count = MODES_COUNT,
	.matrix = (uint8_t[GLOCK_MATRIX_SIZE(MODES_COUNT)]){
		// PO    LFR    LBR    LFW    LBW    SO
		   1,                                    // PO
		   1,      1,                            // LFR
		   0,      1,     1,                     // LBR
		   1,      1,     0,     1,              // LFW
		   0,      0,     1,     0,     1,       // LBW
		   1,      1,     1,     1,     1,    1, // SO
	},
};