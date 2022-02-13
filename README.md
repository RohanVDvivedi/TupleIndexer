# TupleIndexer
A c library that enables you to build a bplus_tree (a b+ tree) for your data, over a data store accessible in fixed sized pages (either persistent or non-persistent store).
The tuple is laid out as per specifications of TupleStore library.

Note: 
 * persistent storage using bufferpool library (not implemented yet).

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [TupleStore](https://github.com/RohanVDvivedi/TupleStore)
 * [BoomPar](https://github.com/RohanVDvivedi/BoomPar)
 * [ReaderWriterLock](https://github.com/RohanVDvivedi/ReaderWriterLock)
 * [Bufferpool](https://github.com/RohanVDvivedi/Bufferpool)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/TupleIndexer.git`

**Build from source :**
 * `cd TupleIndexer`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-ltupleindexer -ltuplestore -lbufferpool -lcutlery` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `<bplus_tree.h>`
   * `<in_memory_data_store.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd TupleIndexer`
 * `sudo make uninstall`
