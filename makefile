# project name
PROJECT_NAME:=tupleindexer

# this is the place where we download in your system
DOWNLOAD_DIR:=/usr/local

# we may download all the public headers

# list of public api headers (only these headers will be installed)
PUBLIC_HEADERS:=bplus_tree/bplus_tree.h bplus_tree/bplus_tree_tuple_definitions_public.h bplus_tree/bplus_tree_iterator_public.h bplus_tree/bplus_tree_walk_down_custom_lock_type.h bplus_tree/bplus_tree_deadlock_avoiding_lock_compatibility_matrix.h\
				array_table/array_table.h array_table/array_table_tuple_definitions_public.h array_table/array_table_range_locker_public.h \
				page_table/page_table.h page_table/page_table_tuple_definitions_public.h page_table/page_table_range_locker_public.h \
				linked_page_list/linked_page_list.h linked_page_list/linked_page_list_tuple_definitions_public.h linked_page_list/linked_page_list_iterator_public.h \
				hash_table/hash_table.h hash_table/hash_table_tuple_definitions_public.h hash_table/hash_table_iterator_public.h hash_table/hash_table_vaccum_params.h \
				sorter/sorter.h sorter/sorter_tuple_definitions_public.h \
				worm/worm.h worm/worm_tuple_definitions_public.h worm/worm_append_iterator_public.h worm/worm_read_iterator_public.h \
				heap_table/heap_table.h heap_table/heap_table_tuple_definitions_public.h heap_table/heap_table_iterator_public.h \
				heap_page/heap_page.h utils/persistent_page_functions.h utils/persistent_page.h utils/persistent_page_access_release.h utils/persistent_page_altered.h utils/persistent_page_altered_util.h utils/persistent_page_unaltered.h utils/persistent_page_unaltered_util.h \
				bitmap_page/bitmap_page.h \
				interface/page_access_methods.h interface/page_access_methods_options.h interface/opaque_page_access_methods.h interface/unWALed_in_memory_data_store.h \
				interface/page_modification_methods.h interface/opaque_page_modification_methods.h interface/unWALed_page_modification_methods.h \
				utils/page_lock_type.h utils/power_table.h utils/bucket_range.h \
				common/page_access_specification.h common/find_position.h common/invalid_tuple_indices.h

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
CFLAGS:=-Wall -O3 -I${INC_DIR}
# linker flags, this will used to compile the binary
LFLAGS:=-L${LIB_DIR} -l${PROJECT_NAME} -ltuplestore -llockking -lcutlery -lpthread
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

PUBLIC_HEADERS_TO_INSTALL=$(patsubst %.h, ${INC_DIR}/${PROJECT_NAME}/%.h, ${PUBLIC_HEADERS})

# install the library, from this directory to user environment path
# you must uninstall current installation before making a new installation
install : uninstall all
	${MK} ${DOWNLOAD_DIR}/include/${PROJECT_NAME}
	for file in $(PUBLIC_HEADERS_TO_INSTALL); do \
	    dir="$$(dirname $$file)"; \
	    dir="$$(echo $$dir | cut -d'/' -f4-)"; \
	    mkdir -p "${DOWNLOAD_DIR}/include/${PROJECT_NAME}/$$dir"; \
	    cp "$$file" "${DOWNLOAD_DIR}/include/${PROJECT_NAME}/$$dir/"; \
	done
	${MK} ${DOWNLOAD_DIR}/lib
	${CP} ${LIB_DIR}/${LIBRARY} ${DOWNLOAD_DIR}/lib
	#${MK} ${DOWNLOAD_DIR}/bin
	#${CP} ${BIN_DIR}/${BINARY} ${DOWNLOAD_DIR}/bin

# removes what was installed
uninstall : 
	${RM} -r ${DOWNLOAD_DIR}/include/${PROJECT_NAME}
	${RM} ${DOWNLOAD_DIR}/lib/${LIBRARY}
	#${RM} ${DOWNLOAD_DIR}/bin/${BINARY}