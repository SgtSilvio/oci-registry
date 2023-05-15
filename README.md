# OCI Registry Java Library

[![GitHub](https://img.shields.io/github/license/sgtsilvio/gradle-metadata?color=brightgreen&style=for-the-badge)](LICENSE)

OCI registry Java library that allows serving OCI artifacts to pull operations.

## How to Use

```kotlin
HttpServer.create()
    .port(5123)
    .handle(OciRegistryHandler(Path.of("path/to/registry/data")))
    .bindNow()
```
