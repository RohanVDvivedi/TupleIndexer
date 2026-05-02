#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<tupleindexer/blob_store/blob_store.h>

#include<tupleindexer/interface/unWALed_in_memory_data_store.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

// attributes of the page_access_specs suggestions for creating page_access_methods
#define PAGE_ID_WIDTH        3
#define PAGE_SIZE          256

// initialize transaction_id and abort_error
const void* transaction_id = NULL;
int abort_error = 0;

typedef struct fix_entry fix_entry;
struct fix_entry
{
	uint32_t unused_space;
	uint64_t page_id;
};

uint32_t fix_entries_size = 0;
fix_entry fix_entries[2048];

void fix_notify(void* context, uint64_t root_page_id, uint32_t unused_space, uint64_t page_id)
{
	fix_entries[fix_entries_size++] = (fix_entry){unused_space, page_id};
}

#define FIX_NOTIFIER &((heap_table_notifier){NULL, fix_notify})

void fix_all_entries(uint64_t root_page_id, const heap_table_tuple_defs* httd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	for(uint32_t i = 0; i < fix_entries_size; i++)
	{
		fix_unused_space_in_heap_table(root_page_id, fix_entries[i].unused_space, fix_entries[i].page_id, httd_p, pam_p, pmm_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
	}
	fix_entries_size = 0;
}

typedef struct blob_pointer blob_pointer;
struct blob_pointer
{
	uint64_t head_page_id;
	uint32_t head_tuple_index;

	uint64_t tail_page_id;
	uint32_t tail_tuple_index;

	uint32_t bytes_appended;
};

blob_pointer bptrs[16];

void append_to_blob(uint64_t root_page_id, blob_pointer* bptr, char* data, uint32_t data_size, const blob_store_tuple_defs* bstd_p, page_access_methods* pam_p, page_modification_methods* pmm_p)
{
	blob_store_write_iterator* bswi_p = get_new_blob_store_write_iterator(root_page_id, bptr->head_page_id, bptr->head_tuple_index, bptr->tail_page_id, bptr->tail_tuple_index, bstd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	while(data_size > 0)
	{
		uint32_t bytes_appended = append_to_tail_in_blob(bswi_p, FIX_NOTIFIER, data, data_size, transaction_id, &abort_error);
		printf("%u bytes appended\n", bytes_appended);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		data += bytes_appended;
		data_size -= bytes_appended;
		bptr->bytes_appended += bytes_appended;
	}

	bptr->head_page_id = get_head_position_in_blob(bswi_p, &(bptr->head_tuple_index));
	uint32_t tail_byte_index;
	bptr->tail_page_id = get_tail_position_in_blob(bswi_p, &(bptr->tail_tuple_index), &tail_byte_index);

	delete_blob_store_write_iterator(bswi_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void discard_from_blob(uint64_t root_page_id, blob_pointer* bptr, uint32_t data_size, const blob_store_tuple_defs* bstd_p, page_access_methods* pam_p, page_modification_methods* pmm_p)
{
	blob_store_write_iterator* bswi_p = get_new_blob_store_write_iterator(root_page_id, bptr->head_page_id, bptr->head_tuple_index, bptr->tail_page_id, bptr->tail_tuple_index, bstd_p, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	while(data_size > 0)
	{
		uint32_t bytes_discarded = discard_from_head_in_blob(bswi_p, FIX_NOTIFIER, data_size, transaction_id, &abort_error);
		printf("%u bytes discarded\n", bytes_discarded);
		if(abort_error)
		{
			printf("ABORTED\n");
			exit(-1);
		}
		data_size -= bytes_discarded;
	}

	bptr->head_page_id = get_head_position_in_blob(bswi_p, &(bptr->head_tuple_index));
	uint32_t tail_byte_index;
	bptr->tail_page_id = get_tail_position_in_blob(bswi_p, &(bptr->tail_tuple_index), &tail_byte_index);

	delete_blob_store_write_iterator(bswi_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void print_blob2(uint64_t page_id, uint32_t tuple_index, uint32_t byte_index, uint32_t batch_size, const blob_store_tuple_defs* bstd_p, page_access_methods* pam_p)
{
	blob_store_read_iterator* bsri_p = get_new_blob_store_read_iterator(page_id, tuple_index, byte_index, bstd_p, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	if(batch_size == 0) // peek and print
	{
		printf("PEEK_PRINT\n");
		while(1)
		{
			uint32_t bytes_peeked;
			const char* bytes = peek_in_blob(bsri_p, &bytes_peeked, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			printf("<%.*s>\n", bytes_peeked, bytes);
			if(bytes_peeked == 0)
				break;
			read_from_blob(bsri_p, NULL, bytes_peeked, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
		}
	}
	else // read and print
	{
		char* batched_bytes = malloc(batch_size);

		printf("READ_PRINT\n");
		while(1)
		{
			uint32_t bytes_read = read_from_blob(bsri_p, batched_bytes, batch_size, transaction_id, &abort_error);
			if(abort_error)
			{
				printf("ABORTED\n");
				exit(-1);
			}
			printf("<%.*s>\n", bytes_read, batched_bytes);
			if(bytes_read == 0)
				break;
		}

		free(batched_bytes);
	}

	delete_blob_store_read_iterator(bsri_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}
}

void print_blob(blob_pointer* bptr, uint32_t batch_size, const blob_store_tuple_defs* bstd_p, page_access_methods* pam_p)
{
	print_blob2(bptr->head_page_id, bptr->head_tuple_index, 0, batch_size, bstd_p, pam_p);
}

char* data1 = "In 1995, a small manufacturing company was struggling to stay afloat. Employee morale was low, production costs were high, and competition was fierce. Then, a new CEO, John, took the helm. He implemented a series of bold changes, including investing in new technology, implementing lean manufacturing principles, and fostering a culture of innovation. The transformation was slow but steady. Within 5 years, the company had reduced production costs by 15% and increased profits by 10%. Over the next 15 years, the company continued to grow and evolve, expanding into new markets and launching innovative products. By 2015, the company had become a global leader in its industry, with a workforce of over 5,000 employees and annual revenue of $1 billion. John's leadership and vision had transformed the company from a struggling enterprise into a thriving success story."
"\t\tDebt can be a major financial burden, hindering your ability to save, invest, and achieve your financial goals. Whether it's credit card debt, student loans, or a mortgage, managing debt requires discipline, planning, and a commitment to living within your means. The first step is to create a budget that accounts for your income, expenses, and debt payments. Identify areas where you can cut back on spending and allocate more money towards debt reduction. Consider using strategies like the snowball method (paying off the smallest debts first) or the avalanche method (tackling debts with the highest interest rates first) to accelerate your progress. If you're struggling to manage your debt, don't hesitate to seek professional help from a financial advisor or credit counselor. They can provide guidance on debt consolidation, negotiation with creditors, and other strategies for getting back on track.";

int main()
{
	/* SETUP STARTED */

	// construct an in-memory data store
	page_access_methods* pam_p = get_new_unWALed_in_memory_data_store(&((page_access_specs){.page_id_width = PAGE_ID_WIDTH, .page_size = PAGE_SIZE}));

	// construct unWALed page_modification_methods
	page_modification_methods* pmm_p = get_new_unWALed_page_modification_methods();

	// construct tuple definitions for blob_store
	blob_store_tuple_defs bstd;
	init_blob_store_tuple_definitions(&bstd, &(pam_p->pas));

	// print the generated blob_store tuple defs
	print_blob_store_tuple_definitions(&bstd);

	// zero initialize bptrs
	for(int i = 0; i < sizeof(bptrs)/sizeof(bptrs[0]); i++)
		bptrs[i] = (blob_pointer){pam_p->pas.NULL_PAGE_ID, 0, pam_p->pas.NULL_PAGE_ID, 0, 0};

	// create a blob_store
	uint64_t root_page_id = get_new_blob_store(&bstd, pam_p, pmm_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// print whole of blob store
	print_blob_store(root_page_id, &bstd, pam_p, transaction_id, &abort_error);

	append_to_blob(root_page_id, &(bptrs[0]), data1 + bptrs[0].bytes_appended, 100, &bstd, pam_p, pmm_p);

	fix_all_entries(root_page_id, &(bstd.httd), pam_p, pmm_p);

	print_blob(&(bptrs[0]), 0, &bstd, pam_p);

	append_to_blob(root_page_id, &(bptrs[0]), data1 + bptrs[0].bytes_appended, 300, &bstd, pam_p, pmm_p);

	fix_all_entries(root_page_id, &(bstd.httd), pam_p, pmm_p);

	print_blob(&(bptrs[0]), 120, &bstd, pam_p);

	append_to_blob(root_page_id, &(bptrs[0]), data1 + bptrs[0].bytes_appended, 200, &bstd, pam_p, pmm_p);

	fix_all_entries(root_page_id, &(bstd.httd), pam_p, pmm_p);

	print_blob(&(bptrs[0]), 0, &bstd, pam_p);

	append_to_blob(root_page_id, &(bptrs[0]), data1 + bptrs[0].bytes_appended, strlen(data1) - bptrs[0].bytes_appended, &bstd, pam_p, pmm_p);

	fix_all_entries(root_page_id, &(bstd.httd), pam_p, pmm_p);

	print_blob(&(bptrs[0]), 50, &bstd, pam_p);

	print_blob_store(root_page_id, &bstd, pam_p, transaction_id, &abort_error);

	/* CLEANUP */
	// destroy blob_store
	destroy_blob_store(root_page_id, &bstd, pam_p, transaction_id, &abort_error);
	if(abort_error)
	{
		printf("ABORTED\n");
		exit(-1);
	}

	// close the in-memory data store
	close_and_destroy_unWALed_in_memory_data_store(pam_p);

	// destroy blob_store_tuple_definitions
	deinit_blob_store_tuple_definitions(&bstd);

	// destory page_modification_methods
	delete_unWALed_page_modification_methods(pmm_p);

	return 0;
}