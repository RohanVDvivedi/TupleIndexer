# Row oriented Tuple Indexer
A c library that enables you to build page_lists (doubly linkedlist of data pages), hash_table (a hash index), bplus_tree (a b+ tree index) for your data, over a data store accessible in fixed sized pages (either persistent or non-persistent store).

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [SimpleTupleStorageModel](https://github.com/RohanVDvivedi/SimpleTupleStorageModel)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/Row-oriented-Tuple-Indexer.git`

**Build from source :**
 * `cd Row-oriented-Tuple-Indexer`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lroti` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `<page_list.h>`
   * `<hash_table.h>`
   * `<bplus_tree.h>`
   * `<data_access_methods.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd Row-oriented-Tuple-Indexer`
 * `sudo make uninstall`
