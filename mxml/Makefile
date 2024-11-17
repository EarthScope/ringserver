
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

# This Makefile is NOT part of Mini-XML, it is distributed with ringserver
# to build the Mini-XML library sources needed by ringserver.
#
# See README-ringserver.md for more information.

MXML_SRCS = $(wildcard ./*.c)
MXML_OBJS = $(MXML_SRCS:.c=.o)

# Mini-XML build options
#CFLAGS +=

LIBA = libmxml.a

all: static

static: $(MXML_OBJS)
	$(AR) rcs $(LIBA) $(MXML_OBJS)

# Standard object building
.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(MXML_OBJS) $(LIBA)
