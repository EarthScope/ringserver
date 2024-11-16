# Mbed TLS in ringserver

The [Mbed TLS](https://github.com/Mbed-TLS/mbedtls) library is used in
ringserver to support TLS/SSL encrypted communications.

## Updating the Mbed TLS version

The Mbed TLS build system is not used in ringserver.  Instead a custom
Makefile is provided in this directory to build the objects that are
directly linked to the ringserver executable.

Only a minimal amount of the Mbed TLS releases are needed by ringserver,
Specifically just the library source and include files.

Steps to update Mbed TLS to a newer release:

1) Download a release bundle from: https://github.com/Mbed-TLS/mbedtls/releases
2) Extract the contents in a temporary directory
3) Copy the following files and directories to this directory:
   - `library/*`
   - `include/*`
   - `LICENSE`
   - `README.md`
