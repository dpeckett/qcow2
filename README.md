# QCOW2

A Go library for reading and writing QCOW2 disk images. QCOW2 is a format used by QEMU and KVM to store virtual machine disk images.

Written based on the official QCOW2 specification: [qcow2.txt](https://gitlab.com/qemu-project/qemu/-/blob/master/docs/interop/qcow2.txt).

## Caveats

The library is not yet complete. It can read and write most QCOW2 images, but some features are not supported:

- Compression (expect for reading DEFLATE)
- Encryption
- Backing files
- External data

You shouldn't use this library in any application that requires data integrity. It has not been tested thoroughly and definitely will result in data loss.