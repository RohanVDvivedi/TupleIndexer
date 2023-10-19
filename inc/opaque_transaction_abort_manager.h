#ifndef OPAQUE_TRANSACTION_ABORT_MANAGER_H
#define OPAQUE_TRANSACTION_ABORT_MANAGER_H

typedef struct transaction_abort_manager transaction_abort_namager;

int is_aborted(const transaction_abort_manager* tam_p);

void mark_aborted(const transaction_abort_manager* tam_p);

#endif