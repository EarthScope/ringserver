# PCRE2 in ringserver

The [PCRE](https://www.pcre.org/) library is used in
ringserver for regular expression support.

## Updating the PCRE2 version

The PCRE2 build system is not used in ringserver.  Instead a custom
Makefile is provided in this directory to build the objects that are
combined into a static library.

Only a minimal number of the PCRE2 release files are needed by ringserver.

Steps to update PCRE2 to a newer release:

1) Download a release bundle from: https://github.com/PCRE2Project/pcre2/releases
2) Extract the contents in a temporary directory
5) Copy the following files and directories to this directory:
   - `NON-AUTOTOOLS-BUILD`
   - `LICENSE`
   - `README`
   - `AUTHORS.md`
   - `src/*.[ch] src/*.generic src/pcre2_chartables.c.dist`
3) Rename `src/pcre2_chartables.c.dist` to `src/pcre2_chartables.c`
4) Rename `src/pcre2.h.generic` to `src/pcre2.h`
5) Rename `src/config.h.generic` to `src/config.h`

Make sure any new files are added and committed.
