# Row oriented Tuple Indexer
A c library that enables you to build a bplus_tree (a b+ tree) for your data, over a data store accessible in fixed sized pages (either persistent or non-persistent store).
The tuple is laid out as per specifications of SimpleTupleStorageModel library.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [SimpleTupleStorageModel](https://github.com/RohanVDvivedi/SimpleTupleStorageModel)
 * [Bufferpool](https://github.com/RohanVDvivedi/Bufferpool)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/Row-oriented-Tuple-Indexer.git`

**Build from source :**
 * `cd Row-oriented-Tuple-Indexer`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lroti -lstupstom -lbufferpool -lcutlery` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `<bplus_tree.h>`
   * `<in_memory_data_store.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd Row-oriented-Tuple-Indexer`
 * `sudo make uninstall`
