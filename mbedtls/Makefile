
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

# This Makefile is NOT part of Mbed TLS, it is distributed with ringserver
# to build the mbedtls library sources needed by ringserver.
#
# See README-ringserver.md for more information.

OBJS = $(SRCS:.c=.o)

MBEDTLS_SRCS = $(wildcard library/*.c)
MBEDTLS_OBJS = $(MBEDTLS_SRCS:.c=.o)

CFLAGS += -Iinclude

all: $(MBEDTLS_OBJS)

clean:
	rm -f $(MBEDTLS_OBJS)
