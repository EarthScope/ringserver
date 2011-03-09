
# Standard compiler parameters
CFLAGS += -D_REENTRANT -I../libmseed -I../mxml -I../pcre 

LDLIBS  = ../pcre/.libs/libpcre.a ../libmseed/libmseed.a ../mxml/libmxml.a -lpthread

# For SunOS/Solaris uncomment the following line
#LDLIBS = ../pcre/.libs/libpcre.a ../libmseed/libmseed.a ../mxml/libmxml.a -lpthread -lsocket -lnsl -lrt

BIN  = ../ringserver

OBJS = stack.o rbtree.o logging.o clients.o slclient.o dlclient.o \
       dsarchive.o mseedscan.o generic.o ring.o ringserver.o

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $(BIN) $(OBJS) $(LDFLAGS) $(LDLIBS)

static: $(OBJS)
	$(CC) $(CFLAGS) -static -o $(BIN) $(OBJS) $(LDFLAGS) $(LDLIBS)

cc:
	@$(MAKE) "CC=$(CC)" "CFLAGS=$(CFLAGS)"

debug:
	@$(MAKE) "CC=$(CC)" "CFLAGS=-g $(CFLAGS)"

clean:
	rm -f $(OBJS) $(BIN)

install:
	@echo
	@echo "No install method"
	@echo "Copy the binary and documentation to desired location"
	@echo
