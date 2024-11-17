# Mini-XML in ringserver

The [Mini-XML](https://www.msweet.org/mxml/) library is used in
ringserver for XML support.

## Updating the Mini-XML version

The Mini-XML build system is not used in ringserver.  Instead a custom
Makefile is provided in this directory to build the objects that are
combined into a static library.

Only a minimal number of the Mini-XML release files are needed by ringserver.

Steps to update Mini-XML to a newer release:

1) Download a release bundle from: https://github.com/michaelrsweet/mxml/releases
2) Extract the contents in a temporary directory
3) Copy the following files and directories to this directory:
   - `LICENSE`
   - `README.md`
   - `mxml*.[ch]`
4) run `./configure --disable-shared --enable-threads` and copy the resulting config.h
   - Look at this config.h, it should be simple with very few settings.

Make sure any new files are added and committed.
