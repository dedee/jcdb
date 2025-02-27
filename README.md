# JCDB - Java CDB Implementation

![build](https://github.com/dedee/jcdb/actions/workflows/build.yml/badge.svg)

## Overview
A Java implementation of Constant Database (CDB), a fast and reliable hash table mechanism for key-value storage originally designed by Daniel J. Bernstein. This implementation focuses on modern Java practices and concurrent access patterns.

## Design Goals
- Use Java NIO for efficient file operations
- Avoid thread synchronization by utilizing FileChannel with offset-based reads, enabling concurrent access from multiple threads
- Maintain constant-time data retrieval performance
- Provide a lightweight and dependency-free solution

## Features
- Thread-safe read operations without synchronization
- Memory-efficient design suitable for large datasets
- Zero-copy reads where possible using NIO
- Immutable database structure ensuring data consistency

## Usage

```java
try (CdbReader reader = CdbReader.create(Path.of("path/to/your/cdb/file.cdb"))) {
    byte[] value = reader.get("key".getBytes());
}
```

## License

Apache 2.0
