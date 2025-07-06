# TupleIndexer
A c library that enables you to build database index structures, thar includes bplus_tree, hash_table, array_table, page_table and linked_page_list.

1. A standard bplus_tree (a b+ tree) for your tuples.

2. I have come up with another data structure that works like an array, it is very similar to Operating System's page tables, but it is optimized to have a compact height and width, by using some clever modifications. It essentially, is a look-up-table from uint64_t (referred to as bucket_id) to your fixed sized tuples (using a modified radix tree-like structure), I am calling this an array_table.

3. Similarly a page_table (derived uaing the above mentioned array_table) structure that provides a dynamic mapping between a 64-bit bucket_id to a page_id. It will be further used to support a hash index (a hash_table) on top of it.

4. There is also an implementation of a linked_page_list, this data structure is a doubly-circular linked list of pages, containing tuples.

5. Together (the page_table and linked_page_list), provides a persistent hash_table data structure.

6. There is a worm (Write Once, Read Multiple) datastructure, allowing you to read/append streams of bytes OR (a more specific usecase of) serialized tuple_def with reference counting. It can allow you to reference-count the on-disk references to some other on-disk data structure. With the worm itself, storing its root page id and information to initialize its tuple definitions. (Here, user provided data boundaires are preserved (upto atmost page size), so make larger writes by bufferring them externally, to reduce internal metadata usage).

7. Finally, there is a sorter, that implements an external merge sort to sort all the tuples. it has optimizations to merge with atmost N runs at a time. It returns a linked page list of the sorted tuples.

All the above data structures for your data, work over a data store accessible in fixed sized pages (either persistent or non-persistent store) using your own page_access_methods (a struct of functions). It also allows you to implement your own page_modification_methods (again a struct of functions) to intercept calls to page modifications in case if you would like to log the changes to pages, to make them redo-able and undo-able.

Additionally all the above data structures are implemented with complete concurrency in mind, and are multi threaded (except the sorter, and only the destroy datastructure calls are not thread safe and needs external locking). They are also designed in a way to always store the root page/head page of the data structure at a fixed page_id (determined at the creation of that datastructure), this allows you to not have to modify the catalog tables (or any other reference store that points to the root and head page ids of these on-disk datastructures), when there are modifications to the referenced data structures.
***NOTE: This library also provides iterators, but they (the iterators) are not thread safe, each new thread/transaction needs its own iterator (a better solution than) OR a lock to protect it.***

Sample implementations of page_access_methods (unWALed_in_memory_data_store.c) and page_modification_methods (unWALed_page_modification_methods.c) have been provided for reference in src/interface directory.

All the indexes and table structures provided are thread safe (using standard latch coupling/crabbing mechanism), when provided with correct and api conforming implementation of your own page_access_methods. Yes, page_access_methods struct will be used by TupleIndexer to request access for a page with a READ_LOCK or a WRITE_LOCK, explicitly.

The tuples on pages and the page headers, are laid out on the page, as per specifications of TupleStore library.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [SerializableInteger](https://github.com/RohanVDvivedi/SerializableInteger)
 * [TupleStore](https://github.com/RohanVDvivedi/TupleStore)
 * [LockKing](https://github.com/RohanVDvivedi/LockKing)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/TupleIndexer.git`

**Build from source :**
 * `cd TupleIndexer`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-ltupleindexer -ltuplestore -llockking -lcutlery -lpthread` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<tupleindexer/bplus_tree/bplus_tree.h>`
   * `#include<tupleindexer/array_table/array_table.h>`
   * `#include<tupleindexer/page_table/page_table.h>`
   * `#include<tupleindexer/linked_page_list/linked_page_list.h>`
   * `#include<tupleindexer/hash_table/hash_table.h>`
   * `#include<tupleindexer/sorter/sorter.h>`
   * `#include<tupleindexer/worm/worm.h>`
   * `#include<tupleindexer/heap_page/heap_page.h>`
   * `#include<tupleindexer/bitmap_page/bitmap_page.h>`
   * `#include<tupleindexer/interface/page_access_methods.h>`
   * `#include<tupleindexer/interface/page_modification_methods.h>`
 * for testing you may need :
   * `#include<tupleindexer/interface/unWALed_in_memory_data_store.h>`
   * `#include<tupleindexer/interface/unWALed_page_modification_methods.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd TupleIndexer`
 * `sudo make uninstall`
