---
title: "Building from Source"
permalink: /docs/dg-building-from-source/
excerpt: "How to configure Hyperspace for your needs."
last_modified_at: 2020-06-23
toc: true
---

## Building from Source

Hyperspace is built with [sbt](https://www.scala-sbt.org/). Run the following commands from the project root directory.

### On *nix OS

To compile:
```
./build/sbt compile
```

To run tests:
```
./build/sbt test
```

### On Windows

Download and install [sbt](https://www.scala-sbt.org/download.html).

To compile:
```
sbt compile
```

To run tests:
```
sbt test
```
### For Development On Intellij
1. Download and install [Intellij](https://www.jetbrains.com/idea/) with [Scala plugin](https://plugins.jetbrains.com/plugin/1347-scala) enabled
2. Intellij -> Open -> Choose file `<root>/build.sbt` -> Open As Project