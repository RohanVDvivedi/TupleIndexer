#ifndef TRANSACTION_ABORT_MANAGER_H
#define TRANSACTION_ABORT_MANAGER_H

typedef struct transaction_abort_manager_methods transaction_abort_manager_methods;
struct transaction_abort_manager_methods
{
	const void* context;

	// returns 0 if not aborted, else returns non zero reason for the abort
	// this method must be thread safe, there could be a transaction, using multiple threads
	int (*is_aborted)(const void* transaction_id, const void* context);

	// abort the transaction, with a reason for abort
	// this functions first marks the trasaction as aborted in your context
	// then it must also put a abort log for the transaction
	// this method must be thread safe, there could be a transaction, using multiple threads
	void (*mark_aborted)(const void* transaction_id, const void* context, int reason);
};

typedef struct transaction_abort_manager transaction_abort_manager;
struct transaction_abort_manager
{
	// transaction_id is of the transaction in progress
	const void* transaction_id;

	// pointer to be shared by all the transactions
	const transaction_abort_manager_methods* tam_methods;
};

#endif