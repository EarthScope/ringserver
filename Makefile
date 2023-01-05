# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

.PHONY: all clean
all clean: pcre2 mxml libmseed src
	$(MAKE) -C src $@

# Test for Makefile/makefile and run make, run configure if needed
.PHONY: pcre2
pcre2:
	@if [ ! -f $@/Makefile -a ! -f $@/makefile ] ; then \
	  ( cd $@ && ./configure --with-link-size=4  \
                                 --with-match-limit=1000 \
                                 --with-match-limit-depth=1000 \
                                 --disable-unicode \
                                 --enable-static --disable-shared ) ; \
	fi
	$(MAKE) -C $@ $(MAKECMDGOALS)

# Test for Makefile/makefile and run make, run configure if needed
.PHONY: mxml
mxml:
	@if [ ! -f $@/Makefile -a ! -f $@/makefile ] ; then \
	  ( cd $@ && ./configure --disable-shared --enable-threads ) ; \
	fi
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: libmseed
libmseed:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: install
install:
	@echo
	@echo "No install method"
	@echo "Copy the binary and documentation to desired location"
	@echo
