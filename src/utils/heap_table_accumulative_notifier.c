#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<stdlib.h>

static int compare_heap_table_accumulative_notifier_entry(const void* he1_p, const void* he2_p)
{
	return compare_numbers(((const heap_table_accumulative_notifier_entry*)he1_p)->page_id, ((const heap_table_accumulative_notifier_entry*)he2_p)->page_id);
}

static cy_uint hash_heap_table_accumulative_notifier_entry(const void* he_p)
{
	return ((const heap_table_accumulative_notifier_entry*)he_p)->page_id;
}

void initialize_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p, uint32_t entries_capacity)
{
	htan_p->entries = malloc(entries_capacity * sizeof(heap_table_accumulative_notifier_entry));
	htan_p->entries_capacity = entries_capacity;

	htan_p->entries_count = 0;

	initialize_singlylist(&(htan_p->free_entries), offsetof(heap_table_accumulative_notifier_entry, free_entries_embed_node));
	initialize_linkedlist(&(htan_p->process_order_for_entries), offsetof(heap_table_accumulative_notifier_entry, process_order_for_entries_embed_node));
	if(!initialize_hashmap(&(htan_p->entries_by_page_id), ELEMENTS_AS_RED_BLACK_BST, 20, &simple_hasher(hash_heap_table_accumulative_notifier_entry), &simple_comparator(compare_heap_table_accumulative_notifier_entry), offsetof(heap_table_accumulative_notifier_entry, entries_by_page_id_embed_node)))
		exit(-1);
}

void deinitialize_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p)
{
	free(htan_p->entries);
}

void push_to_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p, uint64_t root_page_id, uint32_t unused_space, uint64_t page_id)
{
	// fetch if one exists
	heap_table_accumulative_notifier_entry* he_p = (heap_table_accumulative_notifier_entry*) find_equals_in_hashmap(&(htan_p->entries_by_page_id), &(const heap_table_accumulative_notifier_entry){.page_id = page_id});

	if(he_p != NULL)
	{
		// just bump the entry to the tail to process it later
		remove_from_linkedlist(&(htan_p->process_order_for_entries), he_p);
		insert_tail_in_linkedlist(&(htan_p->process_order_for_entries), he_p);
	}
	else if(!is_empty_singlylist(&(htan_p->free_entries))) // allocate with new one from the free list
	{
		// fetch one from free list's head
		heap_table_accumulative_notifier_entry* he_p = (heap_table_accumulative_notifier_entry*) get_head_of_singlylist(&(htan_p->free_entries));

		// remove head from free list
		remove_head_from_singlylist(&(htan_p->free_entries));

		// initialize it with new entry params
		he_p->root_page_id = root_page_id;
		he_p->unused_space = unused_space;
		he_p->page_id = page_id;

		// insert it in the active datastructuires to track it by page_id and in process_order linkedlist
		insert_tail_in_linkedlist(&(htan_p->process_order_for_entries), he_p);
		insert_in_hashmap(&(htan_p->entries_by_page_id), he_p);
	}
	else if(htan_p->entries_count < htan_p->entries_capacity)
	{
		// create one from the newly unused entries
		he_p = htan_p->entries + (htan_p->entries_count++);

		// initialize it to the fullest
		he_p->root_page_id = root_page_id;
		he_p->unused_space = unused_space;
		he_p->page_id = page_id;
		initialize_slnode(&(he_p->free_entries_embed_node));
		initialize_llnode(&(he_p->process_order_for_entries_embed_node));
		initialize_bstnode(&(he_p->entries_by_page_id_embed_node));

		// insert it in the active datastructuires to track it by page_id and in process_order linkedlist
		insert_tail_in_linkedlist(&(htan_p->process_order_for_entries), he_p);
		insert_in_hashmap(&(htan_p->entries_by_page_id), he_p);
	}
	else // do nothing
	{
		return;
	}
}

int pop_from_heap_table_accumulative_notifier(heap_table_accumulative_notifier* htan_p, uint64_t* root_page_id, uint32_t* unused_space, uint64_t* page_id)
{
	heap_table_accumulative_notifier_entry* he_p = (heap_table_accumulative_notifier_entry*) get_head_of_linkedlist(&(htan_p->process_order_for_entries));
	if(he_p == NULL)
		return 0;

	(*root_page_id) = he_p->root_page_id;
	(*unused_space) = he_p->unused_space;
	(*page_id) = he_p->page_id;

	// remove it from being active
	remove_from_linkedlist(&(htan_p->process_order_for_entries), he_p);
	remove_from_hashmap(&(htan_p->entries_by_page_id), he_p);

	// insert it in the free list
	insert_tail_in_singlylist(&(htan_p->free_entries), he_p);

	return 1;
}