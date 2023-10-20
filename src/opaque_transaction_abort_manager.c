#include<opaque_transaction_abort_manager.h>
#include<transaction_abort_manager.h>

int is_aborted(const transaction_abort_manager* tam_p)
{
	return tam_p->tam_methods->is_aborted(tam_p->transaction_id, tam_p->tam_methods->context);
}

void mark_aborted(const transaction_abort_manager* tam_p, int abort_error)
{
	tam_p->tam_methods->mark_aborted(tam_p->transaction_id, tam_p->tam_methods->context, abort_error);
}