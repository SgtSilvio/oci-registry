# OCI Registry Java Library

[![GitHub](https://img.shields.io/github/license/sgtsilvio/gradle-metadata?color=brightgreen&style=for-the-badge)](LICENSE)

OCI registry Java library that allows serving OCI artifacts to pull operations.

## How to Use

### Add the Dependency

Add the following to your `build.gradle(.kts)`:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.sgtsilvio:oci-registry:0.1.0")
}
```

### Start a Reactor HTTP Server with the Handler

Add the following to your code (example in Kotlin):

```kotlin
HttpServer.create()
    .port(1234)
    .handle(OciRegistryHandler(Path.of("path/to/registry/data")))
    .bindNow()
```
