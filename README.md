# Yet Another Benchmark Framework(YABF).

`YABF` is a benchmark framework similar to [`YCSB`][ycsb-github].

YCSB is a great framework in benchmarking database. In its implementation, it uses synchronized interfaces as database interface layer and system thread as execution unit. This puts an inherent limit on concurrency, which is significant when benchmarking high throughput and high latency systems. YABF tends to combine the design of YCSB and the concurrency facilities provided by Golang. It implements main features of YCSB, and provides particular optimizations for these high throughput/latency systems.

## Features

`YABF` support all main features and options as `YCSB`, which include:

1. test client with a varity of database bindings
1. workload and various workload distributions

## Build

Thrift is used in some database binding of `YABF`. To build `YABF`, you need to have the thrift generator(with Golang support) installed. Please refer to [thrift documentation][thrift-doc] for more info on how to install thrift. After the installation, make sure the command `thrift` in `PATH`.

Then get the source code(following the Golang convention, you have to prepare the `GOPATH`) and build it with these commands:

```shell
$ go get github.com/hhkbp2/yabf
$ cd github.com/hhkbp2/yabf
$ make
```

When the build process is done, you could get the `YABF` binary located at `main/yabf`. 

To clean up the build, use this command:

```shell
$ make clean
```

## Usage

Copy `yabf` binary into `PATH` and use it in the following scenarios:

Example 1: Run as interactive shell

`YABF` contains two dummy database bindings by default. The binding of name `simple` just does nothing, which could be used as a silent database binding to verify the `YABF` logic/workload loading. The binding of name `basic` does nothing but echo every operation, which is handy in interactive shell.

You could enter interactive shell and operation on any supported database binding. Take `basic` as an example:

```shell
$ ./yabf shell basic
YABF Command Line Client
Type "help" for command line help
Connected.
> help
Commands
  read key [field1 field2 ...] - Read a record
  scan key recordcount [field1 field2 ...] - Scan starting at key
  insert key name1=value1 [name2=value2 ...] - Insert a new record
  update key name1=value1 [name2=value2 ...] - Update a record
  delete key - Delete a record
  table [tablename] - Get or [set] the name of the table
  quit - Quit
> table test
Using table "test"
0 ms
> insert k c1=v1 c2=v2
Result: OK
1 ms
> read k
Return code: OK
0 ms
> 
```

Example 2: Run a specified workload

`YABF` support workload and various properties to customize the workload as needed. A usual process of testing a database would be:

1. load a data set into the database
```shell
$ yabf load [database binding] [host, port, user, password and other parameters]
```

2. run the specified workload to test the performance

```shell
$ yabf run [database binding] [host, port, user, password and other parameters]
```

`YABF` support a varity of properties are support to customize the workload, e.g.

```shell
yabf load cloudtable \
  -s \
  -p workload=CoreWorkload \
  -p recordcount=100000000 \
  -p threadcount=2000000 \
  -p operationcount=30000000 \
  -p insertcount=4000000 \
  -p readproportion=0.2
  -p updateproportion=0.65
  -p insertproportion=0.15
  -p core_workload_insertion_retry_limit=1 \
  -p cloudtable.host=localhost \
  -p cloudtable.port=2000 \
  -p cloudtable.namespace=test \
  -p table=test.test \
  -p cloudtable.columnfamily=f1 \
  -p cloudtable.authuser=user \
  -p cloudtable.authpassword=password \
  -p fieldcount=1 \
  -p keyprefix=
```

This command specifies the record total to 100 000 000, concurrent operation thread number to 2 000 000, max operation number is 30 000 000, max insertion number is 4 000 000, read/update/insert propotion to 0.2, 0.65, 0.15, and other properties to test the performance of [cloudtable][cloudtable-post] binding.

After the test process is finished, `YABF` would output a summary report of the whole test.

[ycsb-github]: https://github.com/brianfrankcooper/YCSB
[thrift-doc]: https://thrift.apache.org/docs/install/
[cloudtable-post]: http://xlambda.com/blog/2015/09/06/cloudtable-a-distrubted-consistent-big-data-store/

