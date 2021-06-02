#ifndef PAGE_LIST_WRITABLE_HANDLE_H
#define PAGE_LIST_WRITABLE_HANDLE_H

// to write to the page list you need a writable_handle
// you effectively lock two pages of the list

typedef struct page_list_writable_handle page_list_writable_handle;
struct page_list_writable_handle
{
	// the page_list that this handle belongs to
	struct page_list_handle* plh;

	// set to 1 if the curr is the first page of the page_list
	// this indicates that prev must be NULL and the page_list_handle is locked
	int is_curr_the_first_page;

	// null if this is the first page in the page_list
	void* prev;

	// pointer to the current locked page in the page_list
	void* curr;
};

void* get_curr_page(page_list_writable_handle* plwh);

void* get_prev_page(page_list_writable_handle* plwh);

// returns 1 if the seek succeeds, else fails with 0, if you are pointing to the end of the page_list
int seek_to_next_page(page_list_writable_handle* plwh);

// inserts a new page before or after the curr_page
int insert_new_page_after_curr_page(page_list_writable_handle* plwh);
int insert_new_page_before_curr_page(page_list_writable_handle* plwh);	// the new page becomes the curr_page

// deletes the page that the curr_page points to
int delete_curr_page(page_list_writable_handle* plwh);

// releases all the locks held, and closes the writable handle
int close_page_list_writable_handle(page_list_writable_handle* plwh);

#endif