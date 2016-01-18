# Yet Another Benchmark Framework(YABF).

`YABF` is a benchmark framework similar to [`YCSB`][ycsb-github].

YCSB is a great framework in benchmarking database. In its implementation, it uses synchronized interfaces as database interface layer and system thread as execution unit. This puts an inherent limit on concurrency, which is significant when benchmarking high throughput and high latency systems. YABF tends to combine the design of YCSB and the concurrency facilities provided by Golang. It implements main features of YCSB, and provides particular optimizations for these high throughput/latency systems.

[ycsb-github]: https://github.com/brianfrankcooper/YCSB

