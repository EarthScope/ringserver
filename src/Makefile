
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

BIN = ../ringserver

SRCS = stack.c rbtree.c logging.c clients.c cmdtoken.c slclient.c dlclient.c \
       http.c dsarchive.c mseedscan.c generic.c ring.c ringserver.c \
       config.c loadbuffer.c infojson.c infoxml.c tls.c auth.c proxyproto.c
OBJS = $(SRCS:.c=.o)

MBEDTLS_OBJS = $(wildcard ../mbedtls/library/*.o)

FEATURE_DEFS = -D_GNU_SOURCE -D_DARWIN_C_SOURCE \
               -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS

INCLUDES = -I../libmseed -I../mxml -I../pcre2/src -I../mbedtls/include

CFLAGS += -std=c11 $(FEATURE_DEFS) $(INCLUDES)

LDLIBS  = ../pcre2/libpcre2.a ../libmseed/libmseed.a ../mxml/libmxml.a -lpthread

# For SunOS/Solaris uncomment the following line
#LDLIBS = ./pcre2/libpcre2.a ../libmseed/libmseed.a ../mxml/libmxml.a -lpthread -lsocket -lnsl -lrt

all: $(BIN)

$(BIN): $(OBJS) $(MBEDTLS_OBJS)
	$(CC) $(CFLAGS) -o $(BIN) $(OBJS) $(MBEDTLS_OBJS) $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(OBJS) $(BIN)

install:
	@echo
	@echo "No install method"
	@echo "Copy the binary and documentation to desired location"
	@echo
