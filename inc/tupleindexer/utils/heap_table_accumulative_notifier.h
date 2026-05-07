#ifndef HEAP_TABLE_ACCUMULATIVE_NOTIFIER_H
#define HEAP_TABLE_ACCUMULATIVE_NOTIFIER_H

#include<tupleindexer/heap_table/heap_table.h>

#include<cutlery/singlylist.h>
#include<cutlery/linkedlist.h>
#include<cutlery/bst.h>
#include<cutlery/hashmap.h>

typedef struct heap_table_accumulative_notifier_entry heap_table_accumulative_notifier_entry;
struct heap_table_accumulative_notifier_entry
{
	uint64_t root_page_id;
	uint64_t page_id;
	uint32_t unused_space;

	slnode free_entries_embed_node;
	llnode process_order_for_entries_embed_node;
	bstnode entries_by_page_id_embed_node;
};

typedef struct heap_table_accumulative_notifier heap_table_accumulative_notifier;
struct heap_table_accumulative_notifier
{
	uint32_t entries_capacity;
	heap_table_accumulative_notifier_entry* entries;

	uint32_t entries_count;

	singlylist free_entries;
	linkedlist process_order_for_entries;
	hashmap entries_by_page_id;
};

void initialize_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p, uint32_t entries_capacity);

void deinitialize_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p);

void push_to_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p, uint64_t root_page_id, uint32_t unused_space, uint64_t page_id);

int pop_from_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p, uint64_t* root_page_id, uint32_t* unused_space, uint64_t* page_id);

#define HEAP_TABLE_ACCUMULATIVE_NOTIFIER(htan_p) ((const heap_table_notifier){(htan_p), (void (*)(void*, uint64_t, uint32_t, uint64_t))push_to_heap_table_accumulative_notifier})

#endif