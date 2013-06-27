lib_LTLIBRARIES = libdbase.la
libdbase_la_SOURCES = dbase.c utils/xmm.c utils/md5.h utils/md5.c utils/timer.h
libdbase_la_CPPFLAGS = -I utils -D_FILE_OFFSET_BITS=64 -Wall
include_HEADERS = dbase.h

#automake 
sbin_PROGRAMS = hichunkd hitrackerd 
hichunkd_SOURCES = hichunkd.c xdbase.h xdbase.c iniparser.c iniparser.h base64chunkdhtml.h \
	dbase.h dbase.c utils/http.h utils/http.c utils/mmtrie.h utils/mmtrie.c utils/db.c \
	utils/logger.h utils/logger.c utils/mtrie.h utils/mtrie.c utils/stime.h utils/stime.c \
	utils/base64.h utils/base64.c utils/iqueue.h utils/iqueue.c utils/xmm.h utils/xmm.c \
	utils/mutex.h utils/mmtree64.h utils/mmtree64.c utils/md5.h utils/md5.c \
	utils/mmqueue.h utils/mmqueue.c utils/db.h
hichunkd_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -Wall
hichunkd_LDFLAGS = -lz -lsbase

hitrackerd_SOURCES = hitrackerd.c iniparser.c iniparser.h base64trackerdhtml.h xmap.h xmap.c \
				 utils/http.h utils/http.c utils/mmtrie.h utils/mmtrie.c utils/db.c \
				 utils/logger.h utils/logger.c utils/mutex.h utils/mtrie.h utils/mtrie.c \
				 utils/stime.h utils/stime.c utils/base64.h utils/base64.c \
				utils/iqueue.h utils/iqueue.c utils/xmm.h utils/xmm.c utils/db.h \
				utils/mmtree64.h utils/mmtree64.c utils/mmtree.h utils/mmtree.c \
				utils/mmqueue.h utils/mmqueue.c
hitrackerd_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -Wall
hitrackerd_LDFLAGS = -lz -lsbase
EXTRA_DIST = wdbase.sh wdbio.sh
