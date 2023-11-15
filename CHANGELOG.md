# Changelog

## 0.1.0
* Initial version

## 0.2.0
* Update microsoft.azure.storage.blob dependency to 11 - thanks [oseku]

## 0.3.0
* Update to Rebus 6
* Implement data bus attachment storage management API

## 0.4.0
* Add options for skipping container creation and updating last read time when using blobs as data bus storage
* Additional configuration overload that makes it possible to pass a fully qualified container URI (incl. SAS token) when configuring data bus storage

## 0.5.0
* Update to Rebus 6 stable
* Hopefully fix race condition when updating last read time of data bus attachments

## 0.5.1
* Fix race condition when reading attachment concurrently and updating last read time

## 0.6.0
* Update microsoft.azure.keyvault.core to 3.0.5 and microsoft.azure.storage.blob to 11.2.3

## 1.0.1
* Update to Rebus 8
* Add blob-based error tracker - enable by going `.Errors(e => e.UseBlobStorage(...))` in the main Rebus configurer
* Update azure.storage.blobs to 12.19.1


[oseku]: https://github.com/oseku