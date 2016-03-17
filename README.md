# Yet Another Benchmark Framework(YABF).

`YABF` is a benchmark framework similar to [`YCSB`][ycsb-github].

YCSB is a great framework in benchmarking database. In its implementation, it uses synchronized interfaces as database interface layer and system thread as execution unit. This puts an inherent limit on concurrency, which is significant when benchmarking high throughput and high latency systems. YABF tends to combine the design of YCSB and the concurrency facilities provided by Golang. It implements main features of YCSB, and provides particular optimizations for these high throughput/latency systems.

## Features

`YABF` support all main features and options as `YCSB`, which include:

1. test client with a varity of database bindings
1. workload and various workload distributions

## Build

Thrift is used in some database binding of `YABF`. To build `YABF`, you need to have the thrift generator(with Golang support) installed. Please refer to [thrift documentation][thrift-doc] for more info on how to install thrift. After the installation, make sure the command `thrift` in `PATH`.

Then get the source code(follow the Golang convention, you have to prepare the `GOPATH`) and build it with these commands:

```shell
$ go get github.com/hhkbp2/yabf
$ cd github.com/hhkbp2/yabf
$ make
```

When the build process is done, you could get the `YABF` tool as a binary located at `main/yabf`. 

To clean up the build, use this command:

```shell
$ make clean
```

[ycsb-github]: https://github.com/brianfrankcooper/YCSB
[thrift-doc]: https://thrift.apache.org/docs/install/

