# TupleIndexer
A c library that enables you to build a bplus_tree (a b+ tree) for your data, over a data store accessible in fixed sized pages (either persistent or non-persistent store) using your own data_access_methods (a struct of functions). It also allows you to implement your own page_modification_methods (again a struct of functions) to intercept calls to page modifications in case if you would like to log the changes to pages, to make them redo-able and undo-able.

Sample implementationd of data_access_methods (unWALed_in_memory_data_store.c) and page_modification_methods (unWALed_page_modification_methods.c) have been provided for reference in src/interface directory.

The Indexes provided (only a b+tree, as of now) are thread safe (using standard latch coupling mechanism), provided correct and api conforming implementation of your own data_access_methods. Yes, data_access_methods struct will be used by TupleIndexer to request access for a page with a READ_LOCK or a WRITE_LOCK, explicitly.

The tuple is laid out as per specifications of TupleStore library.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [SerializableInteger](https://github.com/RohanVDvivedi/SerializableInteger)
 * [TupleStore](https://github.com/RohanVDvivedi/TupleStore)
 * [ReaderWriterLock](https://github.com/RohanVDvivedi/ReaderWriterLock)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/TupleIndexer.git`

**Build from source :**
 * `cd TupleIndexer`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-ltupleindexer -ltuplestore -lrwlock -lserint -lcutlery -lpthread` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<bplus_tree.h>`
   * `#include<data_access_methods.h>`
   * `#include<page_modification_methods.h>`
 * for testing you may need :
   * `#include<unWALed_in_memory_data_store.h>`
   * `#include<unWALed_page_modification_methods.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd TupleIndexer`
 * `sudo make uninstall`
