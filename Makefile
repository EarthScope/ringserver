# Build environment can be configured the following
# environment variables:
#   CC : Specify the C compiler to use
#   CFLAGS : Specify compiler options to use

.PHONY: all clean
all clean: pcre2 mxml libmseed mbedtls
	$(MAKE) -C src $@

.PHONY: pcre2
pcre2:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: mxml
mxml:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: libmseed
libmseed:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: mbedtls
mbedtls:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: install
install:
	@echo
	@echo "No install method"
	@echo "Copy the binary and documentation to desired location"
	@echo
