# OCI Registry Java Library

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.sgtsilvio/oci-registry/badge.svg?style=for-the-badge)](https://central.sonatype.com/artifact/io.github.sgtsilvio/oci-registry)
[![javadoc](https://javadoc.io/badge2/io.github.sgtsilvio/oci-registry/javadoc.svg?style=for-the-badge)](https://javadoc.io/doc/io.github.sgtsilvio/oci-registry)
[![GitHub](https://img.shields.io/github/license/sgtsilvio/oci-registry?color=brightgreen&style=for-the-badge)](LICENSE)
[![GitHub Workflow Status (with branch)](https://img.shields.io/github/actions/workflow/status/sgtsilvio/oci-registry/check.yml?branch=main&style=for-the-badge)](https://github.com/SgtSilvio/oci-registry/actions/workflows/check.yml?query=branch%3Amain)

OCI registry Java library that allows serving OCI artifacts to pull operations.

## How to Use

### Add the Dependency

Add the following to your `build.gradle(.kts)`:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.sgtsilvio:oci-registry:0.4.0")
}
```

### Start a Reactor HTTP Server with the Handler

Add the following to your code (example in Kotlin):

```kotlin
HttpServer.create()
    .port(1234)
    .handle(OciRegistryHandler(DistributionRegistryStorage(Path.of("path/to/registry/data"))))
    .bindNow()
```
