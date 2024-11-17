
# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

# This Makefile is NOT part of PCRE2, it is distributed with ringserver
# to build the PCRE2 library sources needed by ringserver.
#
# See README-ringserver.md for more information.

# Needed sources are identified in PCRE2's NON-AUTOTOOLS-BUILD file
PCRE2_SRCS = src/pcre2_auto_possess.c src/pcre2_chkdint.c \
	src/pcre2_chartables.c src/pcre2_compile.c src/pcre2_config.c \
	src/pcre2_context.c src/pcre2_convert.c src/pcre2_dfa_match.c \
	src/pcre2_error.c src/pcre2_extuni.c src/pcre2_find_bracket.c \
	src/pcre2_jit_compile.c src/pcre2_maketables.c src/pcre2_match.c \
	src/pcre2_match_data.c src/pcre2_newline.c src/pcre2_ord2utf.c \
	src/pcre2_pattern_info.c src/pcre2_script_run.c src/pcre2_serialize.c \
	src/pcre2_string_utils.c src/pcre2_study.c src/pcre2_substitute.c \
	src/pcre2_substring.c src/pcre2_tables.c src/pcre2_ucd.c \
	src/pcre2_valid_utf.c src/pcre2_xclass.c
PCRE2_OBJS = $(PCRE2_SRCS:.c=.o)

# PCRE2 build options
CFLAGS += -Isrc -DHAVE_CONFIG_H -DSUPPORT_PCRE2_8 -DPCRE2_CODE_UNIT_WIDTH=8 \
	-DPCRE2_STATIC -DMATCH_LIMIT=1000 -DMATCH_LIMIT_DEPTH=1000

LIBA = libpcre2.a

all: static

static: $(PCRE2_OBJS)
	$(AR) rcs $(LIBA) $(PCRE2_OBJS)

# Standard object building
.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(PCRE2_OBJS) $(LIBA)
