# project name
PROJECT_NAME:=tupleindexer

# this is the place where we download in your system
DOWNLOAD_DIR:=/usr/local

# we may download all the public headers

# list of public api headers (only these headers will be installed)
PUBLIC_HEADERS:=bplus_tree/bplus_tree.h bplus_tree/bplus_tree_tuple_definitions_public.h bplus_tree/bplus_tree_iterator_public.h \
				page_table/page_table.h page_table/page_table_tuple_definitions_public.h page_table/page_table_range_locker_public.h page_table/page_table_bucket_range.h \
				linked_page_list/linked_page_list.h linked_page_list/linked_page_list_tuple_definitions_public.h linked_page_list/linked_page_list_iterator_public.h\
				hash_table/hash_table.h hash_table/hash_table_tuple_definitions_public.h hash_table/hash_table_iterator_public.h \
				interface/page_access_methods.h interface/page_access_methods_options.h interface/opaque_page_access_methods.h interface/unWALed_in_memory_data_store.h\
				interface/page_modification_methods.h interface/opaque_page_modification_methods.h interface/unWALed_page_modification_methods.h \
				utils/persistent_page.h utils/power_table_uint64.h\
				common/page_access_specification.h common/find_position.h

# the library, which we will create
LIBRARY:=lib${PROJECT_NAME}.a
# the binary, which will use the created library
BINARY:=${PROJECT_NAME}

# list of all the directories, in the project
INC_DIR:=./inc
OBJ_DIR:=./obj
LIB_DIR:=./lib
SRC_DIR:=./src
BIN_DIR:=./bin

# compiler
CC:=gcc
# compiler flags
CFLAGS:=-Wall -O3 $(addprefix -I,$(sort $(dir $(shell find ${INC_DIR} -name '*.h'))))
# linker flags, this will used to compile the binary
LFLAGS:=-L${LIB_DIR} -l${PROJECT_NAME} -ltuplestore -lrwlock -lserint -lcutlery -lpthread
# Archiver
AR:=ar rcs

# utility
RM:=rm -rf
MK:=mkdir -p
CP:=cp

# sources and objects must be evaluated every time you use them
# figure out all the sources in the project
SOURCES=$(shell find ${SRC_DIR} -name '*.c')
# and the required objects to be built, as intermediary
OBJECTS=$(patsubst ${SRC_DIR}/%.c, ${OBJ_DIR}/%.o, ${SOURCES})
# name of directories of the objects
OBJECTS_DIRS=$(sort $(dir $(OBJECTS)))

# rule to make the directory for storing object files, that we create
${OBJ_DIR}/% :
	${MK} $@

# generic rule to build any object file, it will depend on the existing of its directory path (i.e. ${@D}, this must use secondar expansion of makefile)
.SECONDEXPANSION:
${OBJECTS} : ${OBJ_DIR}/%.o : ${SRC_DIR}/%.c | $${@D}
	${CC} ${CFLAGS} -c $< -o $@

# rule to make the directory for storing libraries, that we create
${LIB_DIR} :
	${MK} $@

# generic rule to make a library
${LIB_DIR}/${LIBRARY} : ${OBJECTS} | ${LIB_DIR}
	${AR} $@ ${OBJECTS}

# rule to make the directory for storing binaries, that we create
${BIN_DIR} :
	${MK} $@

# generic rule to make a binary using the library that we just created
${BIN_DIR}/${BINARY} : ./main.c ${LIB_DIR}/${LIBRARY} | ${BIN_DIR}
	${CC} ${CFLAGS} $< ${LFLAGS} -o $@

# to build the binary along with the library, if your project has a binary aswell
#all : ${BIN_DIR}/${BINARY}
# else if your project is only a library use this
all : ${LIB_DIR}/${LIBRARY}

# clean all the build, in this directory
clean :
	${RM} ${BIN_DIR} ${LIB_DIR} ${OBJ_DIR}

# -----------------------------------------------------
# INSTALLING and UNINSTALLING system wide
# -----------------------------------------------------

PUBLIC_HEADERS_TO_INSTALL=$(patsubst %.h, ${INC_DIR}/%.h, ${PUBLIC_HEADERS})

# install the library, from this directory to user environment path
# you must uninstall current installation before making a new installation
install : uninstall all
	${MK} ${DOWNLOAD_DIR}/include
	${CP} ${PUBLIC_HEADERS_TO_INSTALL} ${DOWNLOAD_DIR}/include
	${MK} ${DOWNLOAD_DIR}/lib
	${CP} ${LIB_DIR}/${LIBRARY} ${DOWNLOAD_DIR}/lib
	#${MK} ${DOWNLOAD_DIR}/bin
	#${CP} ${BIN_DIR}/${BINARY} ${DOWNLOAD_DIR}/bin

PUBLIC_HEADERS_TO_UNINSTALL=$(patsubst %.h, ${DOWNLOAD_DIR}/include/%.h, $(notdir ${PUBLIC_HEADERS}))

# ** assumption is that all your public headers, libraries and binaries used 
# ** will always have your project name in them
# and this is how we figure out what to remove from the 
uninstall : 
	${RM} ${PUBLIC_HEADERS_TO_UNINSTALL}
	${RM} ${DOWNLOAD_DIR}/lib/${LIBRARY}
	#${RM} ${DOWNLOAD_DIR}/bin/${BINARY}